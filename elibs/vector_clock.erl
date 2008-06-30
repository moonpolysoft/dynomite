-module (vector_clock).
-export ([create/1, increment/2]).

create(NodeName) -> {NodeName, 1}.

increment(NodeName, [{NodeName, Version}|Clocks]) ->
	[{NodeName, Version+1}|Clocks];

increment(NodeName, [NodeClock|Clocks]) ->
	[NodeClock|increment(NodeName, Clocks)];
	
increment(NodeName, []) ->
	[{NodeName, 1}].