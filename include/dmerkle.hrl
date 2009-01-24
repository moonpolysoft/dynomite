-define(VERSION, 1).
-define(HEADER_SIZE, 125).

-record(node, {m=0, keys=[], children=[], offset=eof}).
-record(leaf, {m=0, values=[], offset=eof}).
-record(free, {offset,pointer=0}).