-define(VERSION, 1).
-define(STATIC_HEADER, 85).

-define(d_from_blocksize(BlockSize), trunc((BlockSize - 17)/16)).  
-define(pointers_from_blocksize(BlockSize), (trunc(math:sqrt(BlockSize)) - 4)).
-define(headersize_from_blocksize(BlockSize), (?STATIC_HEADER + ?pointers_from_blocksize(BlockSize) * 8)).

-record(node, {m=0, keys=[], children=[], offset=eof}).
-record(leaf, {m=0, values=[], offset=eof}).
-record(free, {offset,pointer=0}).