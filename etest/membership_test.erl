-include_lib("eunit.hrl").


all_test_() ->
  {foreach,
    fun() -> test_setup() end,
    fun(V) -> test_teardown(V) end,
  [
    {"test_write_membership_to_disk", ?_test(test_write_membership_to_disk())},
    {"test_load_membership_from_disk", ?_test(test_load_membership_from_disk())},
    {"test_join_one_node", ?_test(test_join_one_node())},
    {"test_membership_gossip_cluster_collision", ?_test(test_membership_gossip_cluster_collision())},
    {"test_replica_nodes", ?_test(test_replica_nodes())},
    {"test_nodes_for_partition", ?_test(test_nodes_for_partition())},
    {"test_servers_for_key", ?_test(test_servers_for_key())},
    {"test_partitions_for_node_all", ?_test(test_partitions_for_node_all())},
    {"test_initial_partition_setup", ?_test(test_initial_partition_setup())},
    {"test_recover_from_old_membership_read", ?_test(test_recover_from_old_membership_read())}
  ]}.

test_write_membership_to_disk() ->
  {ok, _} = membership:start_link(node(), [node()]),
  ?debugFmt("~p", [data_file()]),
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
  
  
%-record(membership, {config, partitions, version, nodes, old_partitions}).
test_recover_from_old_membership_read() ->
  P = partitions:create_partitions(6, a, [a, b, c, d, e, f]),
  OldMem = {membership, {config, 1, 2, 3, 4}, P, [{a, 1}, {b, 1}], [a, b, c, d, e, f], undefined},
  ok = file:write_file(data_file("a"), term_to_binary(OldMem)),
  {ok, _} = membership:start_link(a, [a, b, c]),
  ?assertEqual(P, membership:partitions()),
  ?assertEqual([a, b, c, d, e, f], membership:nodes()).

test_join_one_node() ->
  mock:expects(sync_manager, load, fun({_, _, P}) -> is_list(P) end, ok),
  mock:expects(storage_manager, load, fun({_, _, P}) -> is_list(P) end, ok),
  {ok, _} = membership:start_link(node(), [node()]),
  membership:join_node(node(), node_a),
  Partitions = membership:partitions(),
  {A, B} = lists:partition(fun({Node,_}) -> Node == node() end, Partitions),
  ?assertEqual(64, length(A) + length(B)),
  membership:stop(),
  verify().

test_membership_gossip_cluster_collision() ->
  mock:expects(sync_manager, load, fun({_, _, P}) -> is_list(P) end, ok, 3),
  mock:expects(storage_manager, load, fun({_, _, P}) -> is_list(P) end, ok, 3),
  {ok, _} = membership:start_link(mem_a, a, [a]),
  {ok, _} = membership:start_link(mem_b, b, [b]),
  gen_server:cast(mem_a, {gossip_with, mem_b}),
  timer:sleep(10),
  Partitions = gen_server:call(mem_a, partitions),
  {A, B} = lists:partition(fun({Node,_}) -> Node == a end, Partitions),
  ?assertEqual(64, length(A) + length(B)),
  ?assert(length(A) > 0),
  ?assert(length(B) > 0),
  membership:stop(mem_a),
  membership:stop(mem_b),
  verify().

test_replica_nodes() ->
  C = configuration:get_config(),
  configuration:set_config(C#config{n=3}),
  {ok, _} = membership:start_link(a, [a, b, c, d, e, f]),
  ?assertEqual([f,a,b], replica_nodes(f)).
  
test_nodes_for_partition() ->
  C = configuration:get_config(),
  configuration:set_config(C#config{n=3}),
  {ok, _} = membership:start_link(a, [a, b, c, d, e, f]),
  ?assertEqual([d,e,f], nodes_for_partition(1)).
  
test_servers_for_key() ->
  C = configuration:get_config(),
  configuration:set_config(C#config{n=3}),
  {ok, _} = membership:start_link(a, [a, b, c, d, e, f]),
  % 25110333
  ?assertEqual([{storage_1, d}, {storage_1, e}, {storage_1, f}], servers_for_key("key")).
  
test_initial_partition_setup() ->
  {ok, _} = membership:start_link(a, [a, b, c, d, e, f]),
  Sizes = partitions:sizes([a,b,c,d,e,f], partitions()),
  ?debugFmt("~p", [Sizes]),
  {value, {c,S}} = lists:keysearch(c, 1, Sizes),
  ?assert(S > 0).
  
test_partitions_for_node_all() ->
  C = configuration:get_config(),
  configuration:set_config(C#config{n=2}),
  {ok, _} = membership:start_link(a, [a, b, c, d, e, f]),
  % 715827883
  Parts = partitions_for_node(a, all),
  PA = partitions_for_node(a, master),
  PF = partitions_for_node(f, master),
  ?debugFmt("Parts ~p", [Parts]),
  ?assertEqual(lists:sort(Parts), lists:sort(PA ++ PF)).

test_partitions_for_node_master() ->
  {ok, _} = membership:start_link(a, [a,b,c,d,e,f]),
  Parts = partitions_for_node(a, master),
  ?assertEqual(10, length(Parts)).
  
test_gossip_server() ->
  ok.

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
  file:delete(data_file()),
  file:delete(data_file("a")),
  file:delete(data_file("b")),
  membership:stop(),
  mock:stop(sync_manager),
  mock:stop(storage_manager),
  configuration:stop().

priv_dir() ->
  Dir = filename:join([t:config(priv_dir), "data", "membership"]),
  filelib:ensure_dir(filename:join(Dir, "membership")),
  Dir.
  
data_file() ->
  filename:join([priv_dir(), atom_to_list(node()) ++ ".bin"]).
  
data_file(Name) ->
  filename:join([priv_dir(), Name ++ ".bin"]).