%%%-------------------------------------------------------------------
%%% File:      sync_manager.erl
%%% @author    Cliff Moon <cliff@powerset.com> [http://www.powerset.com/]
%%% @copyright 2008 Cliff Moon
%%% @doc  
%%%
%%% @end  
%%%
%%% @since 2008-10-21 by Cliff Moon
%%%-------------------------------------------------------------------
-module(sync_manager).
-author('cliff@powerset.com').

-behaviour(gen_server).

%% API
-export([start_link/0, sync/3, done/1, running/0, running/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {running}).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% @spec start_link() -> {ok,Pid} | ignore | {error,Error}
%% @doc Starts the server
%% @end 
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, sync_manager}, ?MODULE, [], []).

sync(Part, NodeA, NodeB) ->
  gen_server:cast(sync_manager, {sync, Part, NodeA, NodeB}).
  
done(Part) ->
  gen_server:cast(sync_manager, {done, Part}).
  
running() ->
  gen_server:call(sync_manager, running).
  
running(Node) ->
  gen_server:call({sync_manager, Node}, running).

%%====================================================================
%% gen_server callbacks
%%====================================================================

%%--------------------------------------------------------------------
%% @spec init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% @doc Initiates the server
%% @end 
%%--------------------------------------------------------------------
init([]) ->
    {ok, #state{running=[]}}.

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
handle_call(running, _From, State = #state{running=Running}) ->
  {reply, Running, State}.

%%--------------------------------------------------------------------
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% @doc Handling cast messages
%% @end 
%%--------------------------------------------------------------------
handle_cast({sync, Part, NodeA, NodeB}, State = #state{running=Running}) ->
  {noreply, State#state{running=lists:keysort(1, 
      lists:keystore(Part, 1, Running, {Part, NodeA, NodeB})
    )}};
    
handle_cast({done, Part}, State = #state{running=Running}) ->
  NewRunning = lists:keydelete(Part, 1, Running),
  {noreply, State#state{running=NewRunning}}.

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
terminate(_Reason, _State) ->
    ok.

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
