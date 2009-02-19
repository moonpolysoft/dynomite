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
-author('cliff@powerset.com').

%% API
-export([get/1, put/3, has_key/1, delete/1]).

-include("config.hrl").
-include("profile.hrl").

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

get(Key) ->
  internal_get(Key, #mediator{config=configuration:get_config()}).
  
put(Key, Context, Value) ->
  internal_put(Key, Context, Value, #mediator{config=configuration:get_config()}).
  
has_key(Key) ->
  internal_has_key(Key, #mediator{config=configuration:get_config()}).

delete(Key) ->
  internal_delete(Key, #mediator{config=configuration:get_config()}).

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------

internal_put(Key, Context, Value, #mediator{config=Config}) ->
  ?prof(internal_put),
  {N,R,W} = unpack_config(Config),
  Servers = membership:servers_for_key(Key),
  Incremented = increment(Context),
  MapFun = fun(Server) ->
    storage_server:put(Server, Key, Incremented, Value)
  end,
  {Good, Bad} = pcall(MapFun, Servers, W),
  Final = if
    length(Good) >= W -> {ok, length(Good)};
    true -> {failure, error_message(Good, Bad, N, W)}
  end,
  ?forp(internal_put),
  Final.
  
internal_get(Key, #mediator{config=Config}) ->
  ?prof(internal_get),
  {N,R,W} = unpack_config(Config),
  Servers = membership:servers_for_key(Key),
  MapFun = fun(Server) ->
    storage_server:get(Server, Key)
  end,
  {Good, Bad} = pcall(MapFun, Servers, R),
  NotFound = resolve_not_found(Bad, R),
  Final = if
    length(Good) >= R -> {ok, resolve_read(Good)};
    NotFound -> {ok, not_found};
    true -> {failure, error_message(Good, Bad, N, R)}
  end,
  ?forp(internal_get),
  Final.
  
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

increment({Pid, undefined}) when is_pid(Pid) ->
  {clobber, vector_clock:create(pid_to_list(Pid))};

increment({Ref, undefined}) ->
  {clobber, vector_clock:create(Ref)};

increment({Pid, Context}) when is_pid(Pid) ->
  vector_clock:increment(pid_to_list(Pid), Context);
  
increment({Ref, Context}) ->
  vector_clock:increment(Ref, Context);
  
increment(Context) ->
  vector_clock:increment(node(), Context).
