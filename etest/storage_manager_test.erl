-include_lib("eunit/include/eunit.hrl").

all_test_() ->
  {foreach,
    fun() -> test_setup() end,
    fun(V) -> test_teardown(V) end,
    [
      {"test_initial_load", ?_test(test_initial_load())},
      {"test_reload_same_layout", ?_test(test_reload_same_layout())},
      {"test_loadout_change", ?_test(test_loadout_change())},
      {"test_loadout_change_with_bootstrap", ?_test(test_loadout_change_with_bootstrap())},
      {"test_unload_servers", ?_test(test_unload_servers())}
    ]}.

test_initial_load() ->
  Partitions = partitions:create_partitions(1, node(), [node()]),
  expect_start_servers([storage_1, storage_2147483649]),
  storage_manager:load(node(), Partitions, lists:map(fun({_,P}) -> P end, Partitions)),
  verify().
  
test_reload_same_layout() ->
  Partitions = partitions:create_partitions(1, node(), [node()]),
  expect_start_servers([storage_1, storage_2147483649]),
  storage_manager:load(node(), Partitions, lists:map(fun({_,P}) -> P end, Partitions)),
  % should not trigger any reload behavior
  storage_manager:load(node(), Partitions, lists:map(fun({_,P}) -> P end, Partitions)),
  verify().
  
test_loadout_change() ->
  Partitions = partitions:create_partitions(0, a, [a]),
  P2 = partitions:create_partitions(1, a, [a]),
  expect_start_servers([storage_1]),
  storage_manager:load(a, Partitions, [1]),
  verify(),
  expect_start_servers([storage_2147483649]),
  storage_manager:load(a, P2, [1, 2147483649]),
  verify().
  
test_loadout_change_with_bootstrap() ->
  P1 = partitions:create_partitions(1, a, [a, b]),
  P2 = partitions:create_partitions(1, a, [a]),
  expect_start_servers([storage_1,storage_2147483649]),
  mock:expects(bootstrap, start, fun({_, Node, _}) -> Node == b end, fun({_, _, CB}, _) -> CB() end),
  storage_manager:load(a, P1, [1]),
  storage_manager:load(b, P2, [1, 2147483649]),
  verify().
  
test_unload_servers() ->
  P1 = partitions:create_partitions(1, a, [a]),
  P2 = partitions:create_partitions(1, a, [a, b]),
  expect_start_servers([storage_1,storage_2147483649]),
  expect_stop_servers([storage_2147483649]),
  storage_manager:load(b, P1, [1, 2147483649]),
  storage_manager:load(a, P2, [1]),
  verify().
  
test_setup() ->
  configuration:start_link(#config{n=0,r=1,w=1,q=6,directory=priv_dir()}),
  {ok, _} = mock:mock(supervisor),
  {ok, _} = mock:mock(bootstrap),
  {ok, _} = storage_manager:start_link().
  
verify() ->
  ok = mock:verify(supervisor),
  ok = mock:verify(bootstrap).
  
test_teardown(_) ->
  storage_manager:stop(),
  mock:stop(supervisor),
  mock:stop(bootstrap).
  
priv_dir() ->
  Dir = filename:join([t:config(priv_dir), "data", "storage_manager"]),
  filelib:ensure_dir(filename:join(Dir, "storage_manager")),
  Dir.
  
expect_start_servers([]) -> ok;

expect_start_servers([Part|Parts]) ->
  mock:expects(supervisor, start_child, fun({storage_server_sup, Spec}) -> 
      element(1, Spec) == Part
    end, ok),
  expect_start_servers(Parts).
  
expect_stop_servers([]) -> ok;

expect_stop_servers([Part|Parts]) ->
  mock:expects(supervisor, terminate_child, fun({storage_server_sup, Name}) ->
      Name == Part
    end, ok),
  mock:expects(supervisor, delete_child, fun({storage_server_sup, Name}) ->
      Name == Part
    end, ok),
  expect_stop_servers(Parts).