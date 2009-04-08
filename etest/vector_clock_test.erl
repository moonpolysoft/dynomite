-include_lib("eunit/include/eunit.hrl").

increment_clock_test() ->
  Clock = create(a),
  Clock2 = increment(b, Clock),
  true = less_than(Clock, Clock2),
  Clock3 = increment(a, Clock2),
  true = less_than(Clock2, Clock3),
  Clock4 = increment(b, Clock3),
  true = less_than(Clock3, Clock4),
  Clock5 = increment(c, Clock4),
  true = less_than(Clock4, Clock5),
  Clock6 = increment(b, Clock5),
  true = less_than(Clock5, Clock6).
  
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

resolve_notfound_test() ->
    ClockVals = {[{a,1}, {b, 2}], ["a", "b"]},
    ClockVals = resolve(not_found, ClockVals),
    ClockVals = resolve(ClockVals, not_found).
    
clock_truncation_test() ->
  Clock = [{a,1},{b,2},{c,3},{d,4},{e,5},{f,6},{g,7},{h,8},{i,9},{j,10},{k,11}],
  Clock1 = truncate(Clock),
  ?assertEqual(10, length(Clock1)),
  ?assertEqual(false, lists:any(fun(E) -> E =:= {a, 1} end, Clock1)).
