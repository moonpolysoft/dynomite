-include_lib("eunit.hrl").


all_test_() ->
  {foreach,
    fun() -> test_setup() end,
    fun(V) -> test_teardown(V) end,
  [
    % {"test_write_membership_to_disk", ?_test(test_write_membership_to_disk())},
    % {"test_load_membership_from_disk", ?_test(test_load_membership_from_disk())},
    % {"test_join_one_node", ?_test(test_join_one_node())},
    {"test_membership_gossip_cluster_collision", ?_test(test_membership_gossip_cluster_collision())}
  ]}.

test_write_membership_to_disk() ->
  {ok, _} = membership:start_link(node(), [node()]),
  {ok, Bin} = file:read_file(data_file()),
  State = binary_to_term(Bin),
  ?assertEqual([node()], State#membership.nodes),
  ?assertEqual(64, length(State#membership.partitions)),
  membership:stop(),
  verify().

test_load_membership_from_disk() ->
  State = create_initial_state(node(), [node()], configuration:get_config()),
  NS = State#membership{version=[a,b,c]},
  file:write_file(data_file(), term_to_binary(NS)),
  {ok, _} = membership:start_link(node(), [node()]),
  ?assertEqual([node()], membership:nodes()),
  ?assertEqual(64, length(membership:partitions())),
  MemState = state(),
  ?assertEqual([a,b,c], MemState#membership.version),
  membership:stop(),
  verify().

test_join_one_node() ->
  mock:expects(sync_manager, load, fun({_, _, P}) -> is_list(P) end, ok),
  mock:expects(storage_manager, load, fun({_, _, P}) -> is_list(P) end, ok),
  {ok, _} = membership:start_link(node(), [node()]),
  membership:join_node(node(), node_a),
  Partitions = membership:partitions(),
  {A, B} = lists:partition(fun({Node,_}) -> Node == node() end, Partitions),
  ?assertEqual(32, length(A)),
  ?assertEqual(32, length(B)),
  membership:stop(),
  verify().

test_membership_gossip_cluster_collision() ->
  mock:expects(sync_manager, load, fun({_, _, P}) -> is_list(P) end, ok, 3),
  mock:expects(storage_manager, load, fun({_, _, P}) -> is_list(P) end, ok, 3),
  {ok, _} = membership:start_link(mem_a, a, [a]),
  {ok, _} = membership:start_link(mem_b, b, [b]),
  ?debugHere,
  gen_server:call(mem_a, {gossip_with, mem_b}),
  Partitions = gen_server:call(mem_a, partitions),
  {A, B} = lists:partition(fun({Node,_}) -> Node == a end, Partitions),
  ?assertEqual(32, length(A)),
  ?assertEqual(32, length(B)),
  membership:stop(mem_a),
  membership:stop(mem_b),
  verify().


test_setup() ->
  configuration:start_link(#config{n=1,r=1,w=1,q=6,directory=priv_dir()}),
  ?assertMatch({ok, _}, mock:mock(sync_manager)),
  ?assertMatch({ok, _}, mock:mock(storage_manager)),
  mock:expects(sync_manager, load, fun({_, _, P}) -> is_list(P) end, ok),
  mock:expects(storage_manager, load, fun({_, _, P}) -> is_list(P) end, ok).
  
verify() ->
  ok = mock:verify(sync_manager),
  ok = mock:verify(storage_manager).
  
test_teardown(_) ->
  mock:stop(sync_manager),
  mock:stop(storage_manager),
  configuration:stop().

priv_dir() ->
  Dir = filename:join([t:config(priv_dir), "data", "membership"]),
  filelib:ensure_dir(filename:join(Dir, "membership")),
  Dir.
  
data_file() ->
  filename:join([priv_dir(), "membership.bin"]).