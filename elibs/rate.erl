%%%-------------------------------------------------------------------
%%% File:      rate.erl
%%% @author    Cliff Moon <cliff@powerset.com> []
%%% @copyright 2008 Cliff Moon
%%% @doc  
%%%
%%% @end  
%%%
%%% @since 2008-08-27 by Cliff Moon
%%%-------------------------------------------------------------------
-module(rate).
-author('cliff@powerset.com').

-behaviour(gen_server).

%% API
-export([start_link/1, get_rate/2, get_datapoints/1, add_datapoint/3, close/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(rate, {period, datapoints}).

-ifdef(TEST).
-include("etest/rate_test.erl").
-endif.

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% @spec start_link() -> {ok,Pid} | ignore | {error,Error}
%% @doc Starts the server
%% @end 
%%--------------------------------------------------------------------
start_link(Period) ->
  gen_server:start_link(?MODULE, Period, []).

get_rate(Pid, OverPeriod) ->
  gen_server:call(Pid, {get_rate, OverPeriod}).

get_datapoints(Pid) ->
  gen_server:call(Pid, datapoints).

add_datapoint(Pid, Value, Time) ->
  gen_server:cast(Pid, {datapoint, {Value, Time}}).
  
close(Pid) ->
  gen_server:cast(Pid, close).

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
init(Period) ->
  {ok, #rate{period=Period,datapoints=queue:new()}}.

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
handle_call({get_rate, OverPeriod}, _From, State = #rate{datapoints=DataPoints,period=Period}) ->
  Limit = epoch() - Period,
  Trimmed = trim_datapoints(Limit,DataPoints),
  Rate = calculate_rate(DataPoints, Period, OverPeriod),
  {reply, Rate, State#rate{datapoints=Trimmed}};
  
handle_call(datapoints, _From, State = #rate{datapoints=DataPoints}) ->
  {reply, queue:to_list(DataPoints), State}.

%%--------------------------------------------------------------------
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% @doc Handling cast messages
%% @end 
%%--------------------------------------------------------------------
handle_cast({datapoint, {Value, Time}}, State = #rate{datapoints=DataPoints,period=Period}) ->
  IntTime = lib_misc:time_to_epoch_int(Time),
  Limit = epoch() - Period,
  ModifiedDP = update(Value, IntTime, trim_datapoints(Limit, DataPoints)),
  % ModifiedDP = [{Value, lib_misc:time_to_epoch_int(Time)} | trim_datapoints(Period, DataPoints)],
  {noreply, State#rate{datapoints=ModifiedDP}};
  
handle_cast(close, State) ->
  {stop, shutdown, State}.

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

update(Value, Time, DataPoints) ->
  case queue:out(DataPoints) of
    {{value, {OldVal, Time}}, ModDP} -> queue:in_r({Value + OldVal, Time}, ModDP);
    _ -> queue:in_r({Value, Time}, DataPoints)
  end.

calculate_rate(DataPoints, Period, OverPeriod) ->
  Sum = lists:foldl(fun({V,_}, Acc) -> Acc+V end, 0, queue:to_list(DataPoints)),
  (Sum*OverPeriod/Period).

trim_datapoints(Limit, DataPoints) ->
  case queue:peek_r(DataPoints) of
    {value, {Val, Time}} when Time < Limit -> trim_datapoints(Limit, queue:drop_r(DataPoints));
    _ -> DataPoints
  end.

epoch() ->
  lib_misc:time_to_epoch_int(now()).
