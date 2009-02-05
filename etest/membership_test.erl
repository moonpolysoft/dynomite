-include_lib("eunit.hrl").


all_test_() ->
  {foreach,
    fun() -> test_setup() end,
    fun(V) -> test_teardown(V) end,
  [
    ?_test(test_write_membership_to_disk()),
    ?_test(test_load_membership_from_disk()),
    ?_test(test_join_one_node())
  ]}.

test_write_membership_to_disk() ->
  membership:start_link(),
  {ok, Bin} = file:read_file(data_file()),
  State = binary_to_term(Bin),
  ?assertEqual([node()], State#membership.nodes),
  ?assertEqual(64, length(State#membership.partitions)),
  membership:stop().

test_load_membership_from_disk() ->
  State = create_initial_state(configuration:get_config()),
  NS = State#membership{version=[a,b,c]},
  file:write_file(data_file(), term_to_binary(NS)),
  membership:start_link(),
  ?assertEqual([node()], membership:nodes()),
  ?assertEqual(64, length(membership:partitions())),
  MemState = state(),
  ?assertEqual([a,b,c], MemState#membership.version),
  membership:stop().

test_join_one_node() ->
  membership:start_link(),
  membership:join_node(node(), node_a),
  Partitions = membership:partitions(),
  {A, B} = lists:partition(fun({Node,_}) -> Node == node() end, Partitions),
  ?assertEqual(32, length(A)),
  ?assertEqual(32, length(B)),
  membership:stop().



test_setup() ->
  configuration:start_link(#config{n=1,r=1,w=1,q=6,directory=priv_dir()}).
  
test_teardown(_) ->
  configuration:stop().

priv_dir() ->
  Dir = filename:join([t:config(priv_dir), "data", "membership"]),
  filelib:ensure_dir(filename:join(Dir, "membership")),
  Dir.
  
data_file() ->
  filename:join([priv_dir(), "membership.bin"]).