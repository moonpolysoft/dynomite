%%%-------------------------------------------------------------------
%%% File:      storage_server.erl
%%% @author    Cliff Moon <> []
%%% @copyright 2008 Cliff Moon
%%% @doc  
%%%
%%% @end  
%%%
%%% @since 2008-04-02 by Cliff Moon
%%%-------------------------------------------------------------------
-module(storage_server).
-author('Cliff Moon').

-behaviour(gen_server).

%% API
-export([start_link/6, start_link/7, get/2, diff/2, get/3, put/4, put/5, fold/3, sync/2, get_tree/1, rebuild_tree/1, has_key/2, has_key/3, delete/2, delete/3, close/1, close/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(storage, {module,table,name,tree,dbkey,blocksize}).

-ifdef(TEST).
-include("etest/storage_server_test.erl").
-endif.

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% @spec start_link(StorageModule) -> {ok,Pid} | ignore | {error,Error}
%% @doc Starts the server
%% @end 
%%--------------------------------------------------------------------
start_link(StorageModule, DbKey, Name, Min, Max, BlockSize) ->
   gen_server:start_link({local, Name}, ?MODULE, {StorageModule,DbKey,Name,Min,Max, BlockSize}, []).
   
start_link(StorageModule, DbKey, Name, Min, Max, BlockSize, OldNode) ->
  {ok, Pid} = start_link(StorageModule, DbKey, Name, Min, Max, BlockSize),
  spawn(fun() -> sync(Pid, {Name, OldNode}) end),
  {ok, Pid}.

get(Name, Key) ->
	get(Name, Key, infinity).
	
get(Name, Key, Timeout) ->
  gen_server:call(Name, {get, Key}, Timeout).
	
put(Name, Key, Context, Value) ->
	put(Name, Key, Context, Value, infinity).
	
put(Name, Key, Context, Value, Timeout) ->
	gen_server:call(Name, {put, Key, Context, Value}, Timeout).
	
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
          storage_server:put(Local, Key, Context, Values);
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
  process_flag(trap_exit, true),
  {ok, Table} = StorageModule:open(DbKey,Name),
  Tree = dmerkle:open(lists:concat([DbKey, "/dmerkle"]), BlockSize),
  {ok, #storage{module=StorageModule,dbkey=DbKey,blocksize=BlockSize,table=Table,name=Name,tree=Tree}}.

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
handle_call({get, Key}, _From, State = #storage{module=Module,table=Table}) ->
  Result = (catch Module:get(sanitize_key(Key), Table)),
  case Result of
    {ok, {Context, Values}} -> 
      stats_server:request(get, lists:foldl(fun(Bin, Acc) -> Acc + byte_size(Bin) end, 0, Values));
    _ -> ok
  end,
	{reply, Result, State};
	
handle_call({put, Key, Context, ValIn}, _From, State = #storage{module=Module,table=Table,tree=Tree}) ->
  Values = lib_misc:listify(ValIn),
  case (catch Module:get(sanitize_key(Key), Table)) of
    {ok, {ReadContext, ReadValues}} ->
      {ResolvedContext, ResolvedValues} = vector_clock:resolve({ReadContext, ReadValues}, {Context, Values}),
      internal_put(Key, ResolvedContext, ResolvedValues, Tree, Table, Module, State);
    not_found -> internal_put(Key, Context, Values, Tree, Table, Module, State);
    Failure -> {reply, Failure, State}
  end;
	
handle_call({has_key, Key}, _From, State = #storage{module=Module,table=Table}) ->
	{reply, catch Module:has_key(sanitize_key(Key),Table), State};
	
handle_call({delete, Key}, _From, State = #storage{module=Module,table=Table,tree=Tree}) ->
  UpdatedTree = dmerkle:delete(Key, Tree),
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
  Reply = Module:fold(Fun, Table, AccIn),
  {reply, Reply, State};
  
handle_call(info, _From, State = #storage{module=Module, table=Table}) ->
  {reply, State, State};
  
handle_call({swap_tree, NewDmerkle}, _From, State = #storage{tree=Dmerkle}) ->
  {reply, ok, State#storage{tree=dmerkle:swap_tree(Dmerkle, NewDmerkle)}};
	
handle_call(rebuild_tree, {FromPid, _Tag}, State = #storage{dbkey=DbKey,table=Table,blocksize=BlockSize,module=Module}) ->
  Parent = self(),
  spawn(fun() -> 
      NewDmerkle = dmerkle:open(lists:concat([DbKey, "/dmerkle_new"]), BlockSize),
      FoldFun = fun({Key, Context, Value}, Dmerkle) ->
        dmerkle:update(Key, Value, Dmerkle)
      end,
      FinalDmerkle = Module:fold(FoldFun, Table, NewDmerkle),
      gen_server:call(Parent, {swap_tree, FinalDmerkle})
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
terminate(_Reason, #storage{module=Module,table=Table,tree=Tree}) ->
  Module:close(Table),
  dmerkle:close(Tree).

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

internal_put(Key, Context, Values, Tree, Table, Module, State) ->
  UpdatedTree = dmerkle:update(Key, Values, Tree),
  case catch Module:put(sanitize_key(Key), Context, Values, Table) of
    {ok, ModifiedTable} ->
      stats_server:request(put, lib_misc:byte_size(Values)),
      {reply, ok, State#storage{table=ModifiedTable,tree=UpdatedTree}};
    Failure -> {reply, Failure, State}
  end.

sanitize_key(Key) when is_atom(Key) -> atom_to_list(Key);
sanitize_key(Key) when is_binary(Key) -> binary_to_list(Key);
sanitize_key(Key) -> Key.
