%%%-------------------------------------------------------------------
%%% File:      stats_server.erl
%%% @author    Cliff Moon <cliff@powerset.com> []
%%% @copyright 2008 Cliff Moon
%%% @doc  
%%%
%%% @end  
%%%
%%% @since 2008-10-24 by Cliff Moon
%%%-------------------------------------------------------------------
-module(stats_server).
-author('cliff@powerset.com').

-behaviour(gen_server).

%% API
-export([start_link/0, rate/3, request/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {get_rate,put_rate,out_rate,in_rate}).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% @spec start_link() -> {ok,Pid} | ignore | {error,Error}
%% @doc Starts the server
%% @end 
%%--------------------------------------------------------------------
start_link() ->
  gen_server:start_link({local, stats_server}, ?MODULE, [], []).

rate(Node, Type, Period) ->
  gen_server:call({stats_server, Node}, {Type, Period}).
  
request(Type, Size) ->
  gen_server:cast(stats_server, {request, Type, Size}).

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
  process_flag(trap_exit, true),
  {ok, GetRate} = rate:start_link(300),
  {ok, PutRate} = rate:start_link(300),
  {ok, OutRate} = rate:start_link(300),
  {ok, InRate} = rate:start_link(300),
  {ok, #state{get_rate=GetRate,put_rate=PutRate,out_rate=OutRate,in_rate=InRate}}.

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
handle_call({get_rate, Period}, _From, State = #state{get_rate=Rate}) ->
  {reply, rate:get_rate(Rate, Period), State};
  
handle_call({put_rate, Period}, _From, State = #state{put_rate=Rate}) ->
  {reply, rate:get_rate(Rate, Period), State};
  
handle_call({in_rate, Period}, _From, State = #state{in_rate=Rate}) ->
  {reply, rate:get_rate(Rate, Period), State};
  
handle_call({out_rate, Period}, _From, State = #state{out_rate=Rate}) ->
  {reply, rate:get_rate(Rate, Period), State}.

%%--------------------------------------------------------------------
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% @doc Handling cast messages
%% @end 
%%--------------------------------------------------------------------
handle_cast({request, get, Size}, State = #state{get_rate=GetRate,out_rate=OutRate}) ->
  rate:add_datapoint(GetRate, 1, now()),
  rate:add_datapoint(OutRate, Size, now()),
  {noreply, State};
  
handle_cast({request, put, Size}, State = #state{put_rate=PutRate,in_rate=InRate}) ->
  rate:add_datapoint(PutRate, 1, now()),
  rate:add_datapoint(InRate, Size, now()),
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
