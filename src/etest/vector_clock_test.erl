-include_lib("eunit.hrl").

increment_clock_test() ->
  Clock = create(a),
  Clock2 = increment(b, Clock),
  [{a, 1}, {b, 1}] = Clock2,
  Clock3 = increment(a, Clock2),
  [{a, 2}, {b, 1}] = Clock3,
  Clock4 = increment(b, Clock3),
  [{a, 2}, {b, 2}] = Clock4,
  Clock5 = increment(c, Clock4),
  [{a, 2}, {b, 2}, {c, 1}] = Clock5,
  Clock6 = increment(b, Clock5),
  [{a, 2}, {b, 3}, {c, 1}] = Clock6.
  
less_than_concurrent_test() ->
  ClockA = [{b, 1}],
  ClockB = [{a, 1}],
  false = less_than(ClockA, ClockB),
  false = less_than(ClockB, ClockA).

less_than_causal_test() ->
  ClockA = [{a,2}, {b,4}, {c,1}],
  ClockB = [{c,1}],
  true = less_than(ClockB, ClockA),
  false = less_than(ClockA, ClockB).
  
less_than_causal_2_test() ->
  ClockA = [{a,2}, {b,4}, {c,1}],
  ClockB = [{a,3}, {b,4}, {c,1}],
  true = less_than(ClockA, ClockB),
  false = less_than(ClockB, ClockA).
  
mixed_up_ordering_test() ->
  ClockA = [{b,4}, {a,2}],
  ClockB = [{a,1}, {b,3}],
  true = less_than(ClockB, ClockA),
  false = less_than(ClockA, ClockB).
  
equivalence_test() ->
  ClockA = [{b,4}, {a,2}],
  ClockB = [{a,2}, {b,4}],
  false = less_than(ClockA, ClockA),
  false = less_than(ClockA, ClockB),
  false = less_than(ClockB, ClockA),
  true = equals(ClockA, ClockA),
  true = equals(ClockB, ClockA),
  true = equals(ClockA, ClockB),
  false = equals(ClockA, [{a,3}, {b,3}]).
  
concurrent_test() ->
  ClockA = [{a,1}],
  ClockB = [{b,1}],
  true = concurrent(ClockA, ClockB).
  
simple_merge_test() ->
  ClockA = [{a,1}],
  ClockB = [{b,1}],
  [{a,1},{b,1}] = merge(ClockA,ClockB).
  
overlap_equals_merge_test() ->
  ClockA = [{a,3},{b,4}],
  ClockB = [{a,3},{c,1}],
  [{a,3},{b,4},{c,1}] = merge(ClockA, ClockB).
  
overlap_unequal_merge_test() ->
  ClockA = [{a,3},{b,4}],
  ClockB = [{a,4},{c,5}],
  [{a,4},{b,4},{c,5}] = merge(ClockA, ClockB).