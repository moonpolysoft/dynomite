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
-export([start_link/3, get/2, get/3, put/4, put/5, has_key/2, has_key/3, delete/2, delete/3, close/1, close/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(storage, {module,table,name}).

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
start_link(StorageModule, DbKey, Name) ->
   gen_server:start_link({local, Name}, ?MODULE, {StorageModule,DbKey,Name}, []).

get(Name, Key) ->
	get(Name, Key, 1000).
	
get(Name, Key, Timeout) ->
  gen_server:call(Name, {get, Key}, Timeout).
	
put(Name, Key, Context, Value) ->
	put(Name, Key, Context, Value, 1000).
	
put(Name, Key, Context, Value, Timeout) ->
	gen_server:call(Name, {put, Key, Context, Value}, Timeout).
	
has_key(Name, Key) ->
	has_key(Name, Key, 1000).
	
has_key(Name, Key, Timeout) ->
	gen_server:call(Name, {has_key, Key}, Timeout).
	
delete(Name, Key) ->
  delete(Name, Key, 1000).
	
delete(Name, Key, Timeout) ->
	gen_server:call(Name, {delete, Key}, Timeout).

close(Name) ->
  close(Name, 1000).
  
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
init({StorageModule,DbKey,Name}) ->
  process_flag(trap_exit, true),
  {ok, #storage{module=StorageModule,table=StorageModule:open(DbKey),name=Name}}.

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
	{reply, catch Module:get(sanitize_key(Key), Table), State};
	
handle_call({put, Key, Context, Value}, _From, State = #storage{module=Module,table=Table}) ->
  case catch Module:put(sanitize_key(Key), Context, Value, Table) of
    {ok, ModifiedTable} -> {reply, ok, State#storage{table=ModifiedTable}};
    Failure -> {reply, Failure, State}
  end;
	
handle_call({has_key, Key}, _From, State = #storage{module=Module,table=Table}) ->
	{reply, catch Module:has_key(sanitize_key(Key),Table), State};
	
handle_call({delete, Key}, _From, State = #storage{module=Module,table=Table}) ->
  case catch Module:delete(sanitize_key(Key), Table) of
    {ok, ModifiedTable} -> 
      {reply, ok, State#storage{table=ModifiedTable}};
    Failure -> {reply, {failure, Failure}, State}
  end;
  
handle_call(info, _From, State = #storage{module=Module, table=Table}) ->
  {reply, State, State};
	
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
terminate(_Reason, #storage{module=Module,table=Table}) ->
  Module:close(Table).

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

sanitize_key(Key) ->
  if
    is_atom(Key) -> atom_to_list(Key);
    is_binary(Key) -> binary_to_list(Key);
    true -> Key
  end.