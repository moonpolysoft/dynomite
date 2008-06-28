-include_lib("include/eunit/eunit.hrl").


nearest_server_test() ->
	1 = nearest_server(0, [1,2,3]),
	2 = nearest_server(1, [1,2,3]),
	3 = nearest_server(2, [1,2,3]),
	first = nearest_server(3, [1,2,3]),
	State = mock_state([{1,one},{2,two},{3,three}]),
	[two] = nearest_server(1, 1, State),
	[one, two] = nearest_server(6, 2, State),
	[one, two, three] = nearest_server(6, 3, State).
	
join_rings_test() ->
  State = mock_state([{1,one},{2,two},{3,three}]),
  SecondState = mock_state([{3,three},{4,four},{5,five}]),
  Merged = int_merge_states(State, SecondState),
  [one, two, three, four, five] = nearest_server(6, 5, Merged).

mock_state(Tuples) ->
	mock_state(Tuples, [], dict:new()).
	
mock_state([], HashRing, Table) ->
	#membership{hash_ring=HashRing,member_table=Table};
	
mock_state([{Code,Name}|Tail], HashRing, Table) ->
	mock_state(Tail, add_nodes_to_ring([Code], HashRing), dict:store(Code, Name, Table)).
	
