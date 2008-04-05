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
-export([start_link/2, get/1, put/2, has_key/1, delete/1, close/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, storage).

-record(storage, {module,table}).

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
start_link(StorageModule, DbKey) ->
   gen_server:start_link({global, ?SERVER}, ?MODULE, {StorageModule,DbKey}, []).

get(Key) ->
	gen_server:call({global, ?SERVER}, {get, Key}).
	
put(Key, Value) ->
	gen_server:call({global, ?SERVER}, {put, Key, Value}).
	
has_key(Key) ->
	gen_server:call({global, ?SERVER}, {has_key, Key}).
	
delete(Key) ->
	gen_server:call({global, ?SERVER}, {delete, Key}).

close() ->
	gen_server:call({global, ?SERVER}, close).

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
init({StorageModule,DbKey}) ->
   {ok, #storage{module=StorageModule,table=StorageModule:open(DbKey)}}.

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
	{reply, Module:get(Key, Table), State};
	
handle_call({put, Key, Value}, _From, State = #storage{module=Module,table=Table}) ->
	{reply, ok, State#storage{table=Module:put(Key,Value,Table)}};
	
handle_call({has_key, Key}, _From, State = #storage{module=Module,table=Table}) ->
	{reply, Module:has_key(Key,Table), State};
	
handle_call({delete, Key}, _From, State = #storage{module=Module,table=Table}) ->
	{reply, ok, State#storage{table=Module:delete(Key,Table)}};
	
handle_call(close, _From, State) ->
	{stop, close, ok, State}.

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
