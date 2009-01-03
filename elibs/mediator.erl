%%%-------------------------------------------------------------------
%%% File:      mediator.erl
%%% @author    Cliff Moon <> []
%%% @copyright 2008 Cliff Moon
%%% @doc  
%%% N = Replication factor of data.
%%% R = Number of hosts that need to participate in a successful read operation
%%% W = Number of hosts that need to participate in a successful write operation
%%% @end  
%%%
%%% @since 2008-04-12 by Cliff Moon
%%%-------------------------------------------------------------------
-module(mediator).
-author('cliff moon').

-behaviour(gen_server).

%% API
-export([start_link/1, get/1, put/3, has_key/1, delete/1, stop/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("config.hrl").

-record(mediator, {config}).
  
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
start_link(Config) ->
  gen_server:start_link({local, mediator}, ?MODULE, Config, []).

get(Key) -> 
  gen_server:call(mediator, {get, Key}).
  
put(Key, Context, Value) -> 
  gen_server:call(mediator, {put, Key, Context, Value}).
  
has_key(Key) -> 
  gen_server:call(mediator, {has_key, Key}).

delete(Key) ->
  gen_server:call(mediator, {delete, Key}).
  
stop() ->
  gen_server:call(mediator, stop).



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
init(Config) ->
    {ok, #mediator{config=configuration:get_config()}}.

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
handle_call({get, Key}, From, State) ->
    spawn_link(fun() ->
                       gen_server:reply(From, internal_get(Key, State))
               end),
    {noreply, State};

handle_call({put, Key, Context, Value}, From, State) ->
    spawn_link(fun() ->
                       gen_server:reply(From, internal_put(Key, Context, Value, State))
               end),
    {noreply, State};

handle_call({has_key, Key}, From, State) ->
    spawn_link(fun() ->
                       gen_server:reply(From, internal_has_key(Key, State))
               end),
    {noreply, State};

handle_call({delete, Key}, From, State) ->
    spawn_link(fun() ->
                       gen_server:reply(From, internal_delete(Key, State))
               end),
    {noreply, State};
  
handle_call(stop, _From, State) ->
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

internal_put(Key, Context, Value, #mediator{config=Config}) ->
  {N,R,W} = unpack_config(Config),
  Servers = membership:servers_for_key(Key),
  Incremented = vector_clock:increment(node(), Context),
  MapFun = fun(Server) ->
    storage_server:put(Server, Key, Incremented, Value)
  end,
  {Good, Bad} = pcall(MapFun, Servers, W),
  if
    length(Good) >= W -> {ok, length(Good)};
    true -> {failure, error_message(Good, Bad, N, W)}
  end.
  
internal_get(Key, #mediator{config=Config}) ->
  {N,R,W} = unpack_config(Config),
  Servers = membership:servers_for_key(Key),
  MapFun = fun(Server) ->
    storage_server:get(Server, Key)
  end,
  {Good, Bad} = pcall(MapFun, Servers, R),
  NotFound = resolve_not_found(Bad, R),
  if
    length(Good) >= R -> {ok, resolve_read(Good)};
    NotFound -> {ok, not_found};
    true -> {failure, error_message(Good, Bad, N, R)}
  end.
  
internal_has_key(Key, #mediator{config=Config}) ->
  {N,R,W} = unpack_config(Config),
  Servers = membership:servers_for_key(Key),
  MapFun = fun(Server) ->
    storage_server:has_key(Server, Key)
  end,
  {Good, Bad} = pcall(MapFun, Servers, R),
  if
    length(Good) >= R -> {ok, resolve_has_key(Good)};
    true -> {failure, error_message(Good, Bad, N, R)}
  end.
  
internal_delete(Key, #mediator{config=Config}) ->
  {N,R,W} = unpack_config(Config),
  Servers = membership:servers_for_key(Key),
  MapFun = fun(Server) ->
    storage_server:delete(Server, Key, 10000)
  end,
  {Good, Bad} = pcall(MapFun, Servers, W),
  if
    length(Good) >= W -> {ok, length(Good)};
    true -> {failure, error_message(Good, Bad, N, W)}
  end.
  
resolve_read([First|Responses]) ->
  case First of
    not_found -> not_found;
    _ -> lists:foldr(fun vector_clock:resolve/2, First, Responses)
  end.
  
resolve_has_key(Good) ->
  {True, False} = lists:partition(fun(E) -> E end, Good),
  if
    length(True) > length(False) -> {true, length(True)};
    true -> {false, length(False)}
  end.
  
resolve_not_found(Bad, R) ->
  Count = lists:foldl(fun({_, E}, Acc) -> 
    case E of
      not_found -> Acc+1;
      _ -> Acc
    end
  end, 0, Bad),
  if
    Count >= R -> true;
    true -> false
  end.
  
pcall(MapFun, Servers, N) ->
  Replies = lib_misc:pmap(MapFun, Servers, N),
  {GoodReplies, Bad} = lists:partition(fun valid/1, Replies),
  Good = lists:map(fun strip_ok/1, GoodReplies),
  % membership:mark_as_bad(lists:map(fun({Server, _}) -> Server end, Bad)),
  {Good, Bad}.
  
valid({ok, _}) -> true;
valid(ok) -> true;
valid(_) -> false.

strip_ok({ok, Val}) -> Val;
strip_ok(Val) -> Val.

error_message(Good, Bad, N, T) ->
  lists:flatten(io_lib:format("contacted ~p of ~p servers.  Needed ~p. Errors: ~w", [length(Good), N, T, Bad])).
  
unpack_config(#config{n=N,r=R,w=W}) ->
  {N, R, W}.
