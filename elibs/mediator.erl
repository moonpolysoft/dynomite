%%%-------------------------------------------------------------------
%%% File:      mediator.erl
%%% @author    Cliff Moon <> []
%%% @copyright 2008 Cliff Moon
%%% @doc  
%%%
%%% @end  
%%%
%%% @since 2008-04-12 by Cliff Moon
%%%-------------------------------------------------------------------
-module(mediator).
-author('cliff moon').

-behaviour(gen_server).

%% API
-export([start_link/1, get/1, put/3, has_key/1, delete/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(mediator, {
    n
  }).
  
-ifdef(TEST).
-include("etest/mediator_test.erl").
-endif.

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% @spec start_link() -> {ok,Pid} | ignore | {error,Error}
%% @doc Starts the server
%% @end 
%%--------------------------------------------------------------------
start_link(N) ->
  gen_server:start_link({local, mediator}, ?MODULE, N, []).

get(Key) -> 
  gen_server:call(mediator, {get, Key}).

put(Key, Context, Value) -> 
  gen_server:call(mediator, {put, Key, Context, Value}).

has_key(Key) -> 
  gen_server:call(mediator, {has_key, Key}).

delete(Key) ->
  gen_server:call(mediator, {delete, Key}).



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
init(N) ->
    {ok, #mediator{n=N}}.

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
handle_call({get, Key}, _From, State) ->
  {reply, {ok, internal_get(Key, State)}, State};
  
handle_call({put, Key, Context, Value}, _From, State) ->
  {reply, internal_put(Key, Context, Value, State), State};
  
handle_call({has_key, Key}, _From, State) ->
  {reply, {ok, internal_has_key(Key, State)}, State};
  
handle_call({delete, Key}, _From, State) ->
  {reply, internal_delete(Key, State), State}.

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

internal_put(Key, Context, Value, #mediator{n=N}) ->
  Servers = membership:server_for_key(Key, N),
  MapFun = fun(Server) ->
    storage_server:put(Server, Key, Context, Value)
  end,
  lib_misc:pmap(MapFun, Servers, []),
  ok.
  
internal_get(Key, #mediator{n=N}) ->
  Servers = membership:server_for_key(Key, N),
  MapFun = fun(Server) ->
    storage_server:get(Server, Key)
  end,
  Responses = lib_misc:pmap(MapFun, Servers, []),
  resolve_read(
    lists:map(fun({ok, Value}) -> Value end, 
      lists:filter(fun(Resp) -> {ok,_} = Resp end, Responses))).
  
internal_has_key(Key, #mediator{n=N}) ->
  Servers = membership:server_for_key(Key, N),
  MapFun = fun(Server) ->
    storage_server:has_key(Server, Key)
  end,
  Responses = lib_misc:pmap(MapFun, Servers, []),
  [{ok,Value}|_] = lists:filter(fun(Resp) -> {ok,_} = Resp end, Responses),
  Value.
  
internal_delete(Key, #mediator{n=N}) ->
  Servers = membership:server_for_key(key, N),
  MapFun = fun(Server) ->
    storage_server:delete(Server, Key)
  end,
  Responses = lib_misc:pmap(MapFun, Servers, []),
  [ok|_] = lists:filter(fun(Resp) -> ok = Resp end, Responses),
  ok.
  
resolve_read(Responses) ->
  [First|Rest] = Responses,
  resolve_read([First], Rest).
  
resolve_read(Maximal, []) ->
  Maximal;

resolve_read(Maximal, [{NextClock,Next}|Responses]) ->
  NewMax = lists:dropwhile(fun({ClockMax, _}) -> 
    left == vector_clock:compare(NextClock, ClockMax)
  end, Maximal),
  Unresolvable = lists:any(fun({ClockMax, _}) ->
    unresolvable == vector_clock:compare(NextClock, ClockMax)
  end, newMax),
  if
    length(NewMax) < length(Maximal) or Unresolvable ->
      FinalMax = [{NextClock,Next}|NewMax];
    true ->
      FinalMax = NewMax
  end,
  resolve_read(FinalMax, Responses).
  
  
  
  
  
  
  
  
  