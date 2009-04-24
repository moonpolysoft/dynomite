%%%-------------------------------------------------------------------
%%% File:      storage_server.erl
%%% @author    Cliff Moon <cliff@powerset.com> []
%%% @copyright 2008 Cliff Moon
%%% @doc  
%%%
%%% @end  
%%%
%%% @since 2008-04-02 by Cliff Moon
%%%-------------------------------------------------------------------
-module(storage_server).
-author('cliff@powerset.com').

-behaviour(gen_server).

%% API
-export([start_link/6, get/2, diff/2, get/3, put/4, put/5, fold/3, sync/2, get_tree/1, rebuild_tree/1, has_key/2, has_key/3, delete/2, delete/3, close/1, close/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(storage, {module,table,name,tree,dbkey,blocksize,cache}).

-include("chunk_size.hrl").
-include("common.hrl").
-include("config.hrl").

-ifdef(TEST).
-include("etest/storage_server_test.erl").
-endif.

-include("profile.hrl").

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% @spec start_link(StorageModule) -> {ok,Pid} | ignore | {error,Error}
%% @doc Starts the server
%% @end 
%%--------------------------------------------------------------------
start_link(StorageModule, DbKey, Name, Min, Max, BlockSize) when is_list(StorageModule) ->
  gen_server:start_link({local, Name}, ?MODULE, {list_to_atom(StorageModule),DbKey,Name,Min,Max, BlockSize}, []);

start_link(StorageModule, DbKey, Name, Min, Max, BlockSize) ->
  gen_server:start_link({local, Name}, ?MODULE, {StorageModule,DbKey,Name,Min,Max, BlockSize}, []).

get(Name, Key) ->
    ?MODULE:get(Name, Key, infinity).
	
get(Name, Key, Timeout) ->
  case gen_server:call(Name, {get, Key}, Timeout) of
    {stream, Pid, Ref} -> stream:recv(Pid, Ref, 200);
    Results -> Results
  end.
	
put(Name, Key, Context, Value) ->
    ?MODULE:put(Name, Key, Context, Value, infinity).
	
put(Name, Key, Context, Value, Timeout) ->
  Size = iolist_size(Value),
  if
    (Size > ?CHUNK_SIZE) and (node(Name) /= node()) -> stream(Name, Key, Context, Value);
    true -> int_put(Name, Key, Context, Value, Timeout)
  end.
	
has_key(Name, Key) ->
	has_key(Name, Key, infinity).
	
has_key(Name, Key, Timeout) ->
	gen_server:call(Name, {has_key, Key}, Timeout).
	
delete(Name, Key) ->
  delete(Name, Key, infinity).
  
rebuild_tree(Name) ->
  gen_server:call(Name, rebuild_tree).
  
diff(Server1, Server2) ->
  Tree1 = get_tree(Server1),
  Tree2 = get_tree(Server2),
  dmerkle:key_diff(Tree1, Tree2).
	
sync(Local, Remote) ->
  TreeA = get_tree(Local),
  TreeB = get_tree(Remote),
  lists:foreach(fun(Key) ->
      RetrieveA = storage_server:get(Local, Key),
      RetrieveB = storage_server:get(Remote, Key),
      case {RetrieveA, RetrieveB} of
        {not_found, {ok, {Context, [Value]}}} -> 
          % error_logger:info_msg("put ~p to local~n", [Key]),
          storage_server:put(Local, Key, Context, Value);
        {{ok, {Context, [Value]}}, not_found} -> 
          % error_logger:info_msg("put ~p to remote~n", [Key]),
          storage_server:put(Remote, Key, Context, Value);
        {not_found, not_found} -> error_logger:info_msg("not found~n");
        {{ok, ValueA}, {ok, ValueB}} ->
          {Context, Values} = vector_clock:resolve(ValueA, ValueB),
          storage_server:put(Remote, Key, Context, Values),
          storage_server:put(Local, Key, Context, Values)
      end
    end, dmerkle:key_diff(TreeA, TreeB)).
	
get_tree(Server) ->
  gen_server:call(Server, get_tree).
	
delete(Name, Key, Timeout) ->
	gen_server:call(Name, {delete, Key}, Timeout).

fold(Name, Fun, AccIn) ->
  gen_server:call(Name, {fold, Fun, AccIn}).

close(Name) ->
  close(Name, infinity).
  
close(Name, Timeout) ->
  gen_server:call(Name, close, Timeout).

%%====================================================================
%% gen_server callbacks
%%====================================================================

%%--------------------------------------------------------------------
%% @spec init({StorageModule,DbKey}) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% @doc Initiates the server
%% @end 
%%--------------------------------------------------------------------
init({StorageModule,DbKey,Name,Min,Max,BlockSize}) ->
    Config = configuration:get_config(),
    load_config_into_dict(Config),
    process_flag(trap_exit, true),  % need to trap exits to deal with merkle issues. gotta do shit the hard way.
    {ok, Table} = StorageModule:open(DbKey,Name),
    DMName = filename:join([DbKey, "dmerkle.idx"]),
    V = if
      BlockSize == undefined; BlockSize == false -> {ok, undefined};
      true -> dmerkle:open(DMName, BlockSize)
    end,
    Tree = case V of
      {ok, T} -> T;
      {error, Reason} -> 
        ?infoFmt("Opening merkle tree failed due to ~p.  Rebuilding.~n", [Reason]),
        do_merkle_tree_rebuild(DMName, DbKey, StorageModule, BlockSize, Table)
    end,
    Storage = #storage{module=StorageModule,dbkey=DbKey,blocksize=BlockSize,table=Table,name=Name,tree=Tree},
    case Config#config.cache of
      true ->
        case (catch cherly:start(Config#config.cache_size)) of
          {ok, C} -> {ok, Storage#storage{cache=C}};
          Error -> 
            ?infoFmt("Cache start failed: ~p~n", [Error]),
            {ok, Storage}
        end;
      _ -> {ok, Storage}
    end.

%%--------------------------------------------------------------------
%% @spec 
%% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% @doc Handling call messages
%% @end 
%%--------------------------------------------------------------------
handle_call({get, Key}, {RemotePid, _Tag}, State = #storage{module=Module,table=Table,cache=C}) ->
  % ?infoFmt("get, ~p, ~p~n", [_Tag, lib_misc:now_float()]),
  ?prof(get),
  ?balance_prof,
  Result = case cache_get(C, sanitize_key(Key)) of
    {ok, [CtxBin|V]} -> 
      {ok, {binary_to_term(CtxBin), V}};
    _Err -> 
      (catch Module:get(sanitize_key(Key), Table))
  end,
  R = case Result of
    {ok, {Context, Values}} -> 
      Size = iolist_size(Values),
      % stats_server:request(get, Size),
      if
        (Size > ?CHUNK_SIZE) and (node(RemotePid) /= node()) ->
          Ref = make_ref(),
          Pid = spawn_link(fun() ->
              stream:send(RemotePid, Ref, {Context, Values})
            end),
          {reply, {stream, Pid, Ref}, State};
        true -> {reply, Result, State}
      end;
    _ -> {reply, Result, State}
  end,
  ?forp(get),
  R;
	
handle_call({put, Key, Context, ValIn}, {_, _Tag}, State = #storage{module=Module,table=Table,tree=Tree}) ->
  % ?infoFmt("put, ~p, ~p~n", [_Tag, lib_misc:now_float()]),
  {Reply, NewState} = inside_process_put(Key, Context, ValIn, State),
  {reply, Reply, NewState};
	
handle_call({has_key, Key}, _From, State = #storage{module=Module,table=Table,cache=C}) ->
  Reply = case cache_get(C, sanitize_key(Key)) of
    {ok, Result} -> {ok, true};
    _ -> (catch Module:has_key(sanitize_key(Key),Table))
  end,
	{reply, Reply, State};
	
handle_call({delete, Key}, _From, State = #storage{module=Module,table=Table,tree=Tree,cache=C}) ->
  UpdatedTree = if
    Tree == undefined -> undefined;
    true -> dmerkle:delete(Key, Tree)
  end,
  cache_remove(C, sanitize_key(Key)),
  case catch Module:delete(sanitize_key(Key), Table) of
    {ok, ModifiedTable} -> 
      {reply, ok, State#storage{table=ModifiedTable,tree=Tree}};
    Failure -> {reply, {failure, Failure}, State}
  end;
  
handle_call(name, _From, State = #storage{name=Name}) ->
  {reply, Name, State};
  
handle_call(get_tree, _From, State = #storage{tree=Tree}) ->
  {reply, Tree, State};
  
handle_call({fold, Fun, AccIn}, _From, State = #storage{module=Module,table=Table}) ->
  spawn_link(fun() -> 
    Reply = Module:fold(Fun, Table, AccIn),
    gen_server:reply(_From, Reply)
  end),
  {noreply, State};
  
handle_call(info, _From, State = #storage{module=Module, table=Table}) ->
  {reply, State, State};
  
% spawn so that we don't block the storage server
handle_call({streaming_put, Ref}, {RemotePid, _Tag}, State) ->
  SS = self(),
  LocalPid = spawn_link(fun() -> 
      case stream:recv(RemotePid, Ref, 200) of
        {ok, {{Key, Context}, Values}} -> 
          Result = storage_server:put(SS, Key, Context, Values),
          RemotePid ! {Ref, put_result, Result};
        {error, timeout} -> {error, timeout}
      end
    end),
  {reply, LocalPid, State};
  
handle_call({swap_tree, NewDmerkle}, _From, State = #storage{tree=Dmerkle}) ->
  {reply, ok, State#storage{tree=dmerkle:swap_tree(Dmerkle, NewDmerkle)}};
	
handle_call(rebuild_tree, {FromPid, _Tag}, State = #storage{dbkey=DbKey,table=Table,blocksize=BlockSize,module=Module,tree=Tree,name=Name}) ->
  Parent = self(),
  DMFile = filename:join([DbKey, "dmerkle.idx"]),
  dmerkle:close(Tree),
  file:delete(DMFile),
  {ok, NewTree} = dmerkle:open(DMFile, BlockSize),
  spawn(fun() ->
      FoldFun = fun({Key, Context, Value}, _) ->
        dmerkle:update(Key, Value, NewTree)
      end,
      FinalDmerkle = Module:fold(FoldFun, Table, whatever),
      ?infoFmt("Finished rebuilding merkle trees for ~p~n", [Name])
    end),
  {reply, ok, State};
	
handle_call(close, _From, State) ->
	{stop, shutdown, ok, State}.

%%--------------------------------------------------------------------
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% @doc Handling cast messages
%% @end 
%%--------------------------------------------------------------------
handle_cast({put, Key, Context, ValIn}, State) ->
  {_, NewState} = inside_process_put(Key, Context, ValIn, State),
  {noreply, NewState};

handle_cast(_Msg, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% @doc Handling all non call/cast messages
%% @end 
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------
%% @spec terminate(Reason, State) -> void()
%% @doc This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%% @end 
%%--------------------------------------------------------------------
terminate(_Reason, #storage{module=Module,table=Table,tree=Tree,cache=C}) ->
  Module:close(Table),
  if
    Tree == undefined -> ok;
    true -> dmerkle:close(Tree)
  end,
  cache_stop(C).

%%--------------------------------------------------------------------
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @doc Convert process state when code is changed
%% @end 
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------

load_config_into_dict(#config{buffered_writes=BufferedWrites}) ->
  put(buffered_writes, BufferedWrites).

int_put(Name, Key, Context, Value, Timeout) ->
  Config = configuration:get_config(),
  case Config#config.buffered_writes of
    true -> 
      gen_server:cast(Name, {put, Key, Context, Value});
    _ -> gen_server:call(Name, {put, Key, Context, Value}, Timeout)
  end.
  
inside_process_put(Key, Context, ValIn, State = #storage{module=Module,table=Table,tree=Tree}) ->
  Values = lib_misc:listify(ValIn),
  ?prof(outer_put),
  R = case Context of
    {clobber, Context2} -> internal_put(Key, Context2, Values, Tree, Table, Module, State);
    _ ->
      case (catch Module:get(sanitize_key(Key), Table)) of
        {ok, {ReadContext, ReadValues}} ->
          {ResolvedContext, ResolvedValues} = vector_clock:resolve({ReadContext, ReadValues}, {Context, Values}),
          internal_put(Key, ResolvedContext, ResolvedValues, Tree, Table, Module, State);
        {ok, not_found} -> internal_put(Key, Context, Values, Tree, Table, Module, State);
        Failure -> {Failure, State}
      end
  end,
  ?forp(outer_put),
  R.
  
% we want to pre-arrange a rendevous so as to not block the storage server
% blocking whomever is local is perfectly ok
stream(Name, Key, Context, Value) ->
  Config = configuration:get_config(),
  Ref = make_ref(),
  Pid = gen_server:call(Name, {streaming_put, Ref}),
  stream:send(Pid, Ref, {{Key, Context}, lib_misc:listify(Value)}),
  receive
    {Ref, put_result, Result} -> Result
  after 1000 ->
    {error, timeout}
  end.

internal_put(Key, Context, Values, Tree, Table, Module, State) ->
  ?balance_prof,
  cache_put(State#storage.cache, sanitize_key(Key), [term_to_binary(vector_clock:truncate(Context)), Values]),
  ?prof(dmerkle_update),
  if
    Tree == undefined -> ok;
    true ->
      dmerkle:updatea(sanitize_key(Key), Values, Tree)
  end,
  ?forp(dmerkle_update),
  ?prof(put),
  TableResult = Module:put(sanitize_key(Key), vector_clock:truncate(Context), Values, Table),
  ?forp(put),
  case TableResult of
    {ok, ModifiedTable} ->
      % stats_server:request(put, iolist_size(Values)),
      {ok, State#storage{table=ModifiedTable}};
    Failure -> {Failure, State}
  end.

cache_put(undefined, _, _) -> ok;
cache_put(C, Key, Values) ->
  cherly:put(C, Key, Values).

cache_get(undefined, _) -> not_found;
cache_get(C, Key) ->
  cherly:get(C, Key).
  
cache_remove(undefined, _) -> ok;
cache_remove(C, Key) ->
  cherly:remove(C, Key).
  
cache_stop(undefined) -> ok;
cache_stop(C) ->
  cherly:stop(C).
  

sanitize_key(Key) when is_atom(Key) -> atom_to_list(Key);
sanitize_key(Key) when is_binary(Key) -> binary_to_list(Key);
sanitize_key(Key) -> Key.

do_merkle_tree_rebuild(DMName, DbKey, StorageModule, BlockSize, Table) ->
  file:delete(DMName),
  file:delete(filename:join([DbKey, "dmerkle.keys"])),
  {ok, T} = dmerkle:open(DMName, BlockSize),
  spawn_link(fun() ->
      Result = (StorageModule:fold(fun({Key,_,Values}, _) -> 
          dmerkle:updatea(Key, Values, T) 
        end, nil, Table))
    end),
  T.