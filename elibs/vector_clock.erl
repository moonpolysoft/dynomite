-module (vector_clock).
-export ([create/1, increment/2, compare/2]).


create(NodeName) -> {NodeName, 1}.

increment(NodeName, [{NodeName, Version}|Clocks]) ->
	[{NodeName, Version+1}|Clocks];

increment(NodeName, [NodeClock|Clocks]) ->
	[NodeClock|increment(NodeName, Clocks)];
	
increment(NodeName, []) ->
	[{NodeName, 1}].
	
	
% will return either left, right, equal, or unresolvable.  This means the following:
% left means that the left clock is most up-to-date
% right means the right right clock is most up-to-date
% equal means that both clocks are equal
% unresolvable means that both clocks are most up to date and cannot be resolved
compare(ClockA, ClockB) ->
  equal.