-include_lib("include/eunit/eunit.hrl").

-define(NODES, [one, two, three, four, five, six, seven, eight]).
-define(PARTITIONS, [{one, 1}, {two, 2}, {three, 3}, {four, 4}, {five, 5}, {six, 6}, {seven, 7}, {eight, 8}]).

partition_range_test() ->
  (2 bsl 21) = partition_range(10),
  (2 bsl 14) = partition_range(17).

create_partitions_length_test() ->
  State = create_initial_state(#config{q=10}),
  Partitions = length(State#membership.partitions),
  Partitions = (2 bsl 9).
  
within_true_test() ->
  {true, one} = within(3, one, three, ?NODES),
  {true, six} = within(3, six, eight, ?NODES),
  {true, six} = within(3, six, seven, ?NODES).
  
within_false_test() ->
  false = within(2, one, three, ?NODES),
  false = within(3, one, eight, ?NODES).
  
within_swapped_test() ->
  {true, one} = within(3, three, one, ?NODES),
  {true, one} = within(3, two, one, ?NODES),
  {true, seven} = within(2, eight, seven, ?NODES).
  
steal_partitions_test() ->
  [{bean, 1}, {bean, 2}, {bean, 3}|_] = steal_partitions(bean, 3, 1, 1, ?NODES, ?PARTITIONS, []).
  
join_node_split_test() ->
  Membership = #membership{
    partitions = create_partitions(10, one),
    config=#config{q=10,n=3},
    version=vector_clock:create(one),
    nodes=[one, two]},
  NewMem = int_join_node(two, Membership),
  Partitions = NewMem#membership.partitions,
  {Ones, Twos} = lists:partition(fun({Node,_}) -> Node == one end, Partitions),
  ?power_2(9) = length(Ones),
  ?power_2(9) = length(Twos),
  SecMem = int_join_node(three, Membership#membership{partitions=Partitions,nodes=[one, two, three]}),
  {Threes, Others} = lists:partition(fun({Node,_}) -> Node == three end, SecMem#membership.partitions),
  341 = length(Threes),
  683 = length(Others),
  ThrMem = int_join_node(four, SecMem#membership{nodes=[one, two, three, four]}),
  {Fours, Others2} = lists:partition(fun({Node,_}) -> Node == four end, ThrMem#membership.partitions),
  256 = length(Fours),
  768 = length(Others2).
  
start_server_test() ->
  {ok, Pid} = start_link(#config{q=10,n=3,r=2,w=2}).
  
find_partition_simple_test() ->
  1 = find_partition(500, 10).
  
find_partition_middle_test() ->
  71303169 = find_partition(71303169+800, 10).
  
find_partition_beginning_test() ->
  1 = find_partition(1, 10).
  
find_partition_end_test() ->
  3590324225 = find_partition(3594518528, 10).
  
find_partition_very_end_test() ->
  4290772993 = find_partition(4294967296, 10).
  
index_for_partition_test() ->
  1 = index_for_partition(1, 10).
  
index_for_partition_end_test() ->
  1024 = index_for_partition(4290772993, 10).
  
n_nodes_whole_test() ->
  [a, b, c] = n_nodes(a, 3, [a, b, c]).
  
n_nodes_simple_test() ->
  [a] = n_nodes(a, 1, [a, b, c]).
  
n_nodes_wrap_test() ->
  [d, e, a] = n_nodes(d, 3, [a, b, c, d, e]).
  
n_nodes_sublist_test() ->
  [b, c] = n_nodes(b, 2, [a, b, c, d, e]).