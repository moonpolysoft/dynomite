-include_lib("eunit/include/eunit.hrl").

all_test_() ->
  {foreach,
    fun() -> test_setup() end,
    fun(V) -> test_teardown(V) end,
    [
      {"test_initial_load", ?_test(test_initial_load())},
      {"test_reload_same_layout", ?_test(test_reload_same_layout())},
      {"test_loadout_change", ?_test(test_loadout_change())},
      {"test_unload_servers", ?_test(test_unload_servers())}
    ]}.

test_initial_load() ->
  P = partitions:create_partitions(1, a, [a]),
  expect_start_servers([sync_1, sync_2147483649]),
  sync_manager:load(a, P, p_for_n(a, P)),
  verify().
  
test_reload_same_layout() ->
  P = partitions:create_partitions(1, a, [a]),
  expect_start_servers([sync_1, sync_2147483649]),
  sync_manager:load(a, P, p_for_n(a, P)),
  sync_manager:load(a, P, p_for_n(a, P)),
  verify().

test_loadout_change() ->
  P1 = partitions:create_partitions(0, a, [a]),
  P2 = partitions:create_partitions(1, a, [a]),
  expect_start_servers([sync_1]),
  sync_manager:load(a, P1, p_for_n(a, P1)),
  verify(),
  expect_start_servers([sync_2147483649]),
  sync_manager:load(a, P2, p_for_n(a, P2)),
  verify().

test_unload_servers() ->
  P1 = partitions:create_partitions(1, a, [a]),
  P2 = partitions:create_partitions(1, a, [a, b]),
  ?debugFmt("p1 ~p", [P1]),
  ?debugFmt("p2 ~p", [P2]),
  expect_start_servers([sync_1,sync_2147483649]),
  expect_stop_servers([sync_1,sync_2147483649]),
  sync_manager:load(b, P1, p_for_n(a, P1)),
  sync_manager:load(a, P2, p_for_n(a, P2)),
  verify().

test_setup() ->
  {ok, _} = sync_manager:start_link(),
  {ok, _} = mock:mock(supervisor).
  
verify() ->
  ok = mock:verify(supervisor).
  
test_teardown(_) ->
  sync_manager:stop(),
  mock:stop(supervisor).
  
p_for_n(N, P) ->
  lists:map(fun({_,P}) -> P end, lists:filter(fun({A,_}) -> A == N end, P)).
  
expect_start_servers([]) -> ok;

expect_start_servers([Part|Parts]) ->
  mock:expects(supervisor, start_child, fun({sync_server_sup, Spec}) -> 
      element(1, Spec) == Part
    end, ok),
  expect_start_servers(Parts).

expect_stop_servers([]) -> ok;

expect_stop_servers([Part|Parts]) ->
  mock:expects(supervisor, terminate_child, fun({sync_server_sup, Name}) ->
      Name == Part
    end, ok),
  mock:expects(supervisor, delete_child, fun({sync_server_sup, Name}) ->
      Name == Part
    end, ok),
  expect_stop_servers(Parts).