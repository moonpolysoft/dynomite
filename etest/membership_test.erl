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
    version=vector_clock:create(one)},
  NewMem = join_node(two, Membership, [one, two]),
  Partitions = NewMem#membership.partitions,
  {Ones, Twos} = lists:partition(fun({Node,_}) -> Node == one end, Partitions),
  ?power_2(9) = length(Ones),
  ?power_2(9) = length(Twos),
  SecMem = join_node(three, Membership#membership{partitions=Partitions}, [one, two, three]),
  {Threes, Others} = lists:partition(fun({Node,_}) -> Node == three end, SecMem#membership.partitions),
  341 = length(Threes),
  683 = length(Others),
  ThrMem = join_node(four, SecMem, [one, two, three, four]),
  {Fours, Others2} = lists:partition(fun({Node,_}) -> Node == four end, ThrMem#membership.partitions),
  256 = length(Fours),
  768 = length(Others2).
  
start_server_test() ->
  {ok, Pid} = start_link(#config{q=10,n=3,r=2,w=2}).