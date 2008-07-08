-include_lib("include/eunit/eunit.hrl").

start_server_test() ->
  {ok, _} = membership:start_link(),
  
  membership:stop().

duplicate_server_test() ->
  State = create_membership_state([{one, two}, {three, four}, {five, six}]),
  KeyHash = erlang:phash2(key),
  [{five,six},{one,two},{three,four}] = nearest_server(KeyHash, 3, State).

nearest_server_test() ->
	1 = nearest_server(0, [1,2,3]),
	2 = nearest_server(1, [1,2,3]),
	3 = nearest_server(2, [1,2,3]),
  first = nearest_server(3, [1,2,3]),
  State = create_membership_state([one, two, three]),
  [one] = nearest_server(1, 1, State),
  [one, two] = nearest_server(6, 2, State),
  [one, two, three] = nearest_server(6, 3, State).
	
join_rings_test() ->
  State = create_membership_state([one, two, three]),
  SecondState = create_membership_state([three, four, five]),
  Merged = int_merge_states(State, SecondState),
  [one, four, two, five, three] = nearest_server(6, 5, Merged).

	
