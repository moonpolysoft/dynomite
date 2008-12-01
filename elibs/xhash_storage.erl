%%%-------------------------------------------------------------------
%%% File:      xhash_storage.erl
%%% @author    Cliff Moon <cliff@powerset.com> []
%%% @copyright 2008 Cliff Moon
%%% @doc  based on: http://www.cs.tau.ac.il/~shanir/nir-pubs-web/Papers/Split-Ordered_Lists.pdf
%%%
%%% @end  
%%%
%%% @since 2008-11-15 by Cliff Moon
%%%-------------------------------------------------------------------
-module(xhash_storage).
-author('cliff@powerset.com').

%% API

-export([open/2, close/1, get/2, put/4, has_key/2, delete/2, fold/3]).

-define(VERSION, 0).
-define(LOAD_LIMIT, 2.0).
-define(INDEX_HEADER_SIZE, 40).
-define(DATA_HEADER_SIZE, 48).
-define(CHUNK_SIZE, 1024).
-define(MAX_CHUNKS, 20).
-define(PARENT(Bucket), if Bucket > 0 -> Bucket bxor (1 bsl trunc(math:log(Bucket)/math:log(2))); true -> 0 end).
-ifdef(DEBUG).
-define(debugmsg(Message), error_logger:info_msg(Message)).
-define(debug(Message, Stuff), error_logger:info_msg(Message, Stuff)).
-endif.
-ifndef(DEBUG).
-define(debugmsg(Message), noop).
-define(debug(Message, Stuff), noop).
-endif.
-record(xhash, {data,index,head,capacity,size,idx_cache,node_cache}).
-record(node, {key=nil, data=nil, node_header}).
-record(node_header, {keyhash=0,keysize=0,datasize=0,nextptr=0}).


-ifdef(TEST).
-include("etest/xhash_storage_test.erl").
-endif.

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% @spec 
%% @doc
%% @end 
%%--------------------------------------------------------------------
open(Directory, Name) ->
  ok = filelib:ensure_dir(Directory ++ "/"),
  DataFileName = lists:concat([Directory, "/", Name, ".xd"]),
  IndexFileName = lists:concat([Directory, "/", Name, ".xi"]),
  case file:open(DataFileName, [read, write, binary, raw]) of
    {ok, DataFile} ->
      case file:open(IndexFileName, [read, write, binary, raw]) of
        {ok, IndexFile} -> 
          initialize_or_verify(#xhash{data=DataFile,index=IndexFile});
        Failure -> 
          file:close(DataFile),
          Failure
      end;
    Failure -> Failure
  end.

close(#xhash{data=Data,index=Index}) ->
  file:close(Data),
  file:close(Index),
  ok.
  
get(Key, XHash = #xhash{capacity=Capacity,data=Data,index=Index,size=Size,head=Head}) ->
  if
    Size == 0 -> {ok, not_found};
    true ->
      UnrevHash = lib_misc:hash(Key),
      KeyHash = lib_misc:reverse_bits(UnrevHash), % bor 16#80000000),
      BucketIndex = UnrevHash rem Capacity,
      ?debug("get(~p) bucket ~p", [Key, BucketIndex]),
      {ReadBucket, Pointer} = read_bucket(BucketIndex, XHash),
      if
        Pointer == 0 -> {ok, not_found};
        true ->
          case find_node(Pointer, Key, KeyHash, XHash) of
            not_found -> {ok, not_found};
            {NewPointer, Header} -> 
              {Key, Context, Values} = read_node_data(NewPointer, Header, Data),
              {ok, {Context, Values}}
          end
      end
  end.
  
put(Key, Context, Values, XHash = #xhash{capacity=Capacity,size=Size,data=Data,index=Index,head=Head}) ->
  ?debug("put(~p, Context, Values, XHash)", [Key]),
  LoadFactor = Size / Capacity,
  XHash1 = if
    LoadFactor > 0.8 -> double_index(XHash);
    true -> XHash
  end,
  UnrevHash = lib_misc:hash(Key),
  KeyHash = lib_misc:reverse_bits(UnrevHash),  %bor 16#80000000),
  Bucket = UnrevHash rem Capacity,
  KeyBin = list_to_binary(Key),
  DataBin = term_to_binary({Context, Values}),
  Header = #node_header{keyhash=KeyHash,keysize=byte_size(KeyBin),datasize=byte_size(DataBin)},
  {_, NewHash} = write_node(Bucket, Header, KeyBin, DataBin, XHash1),
  {ok, NewHash}.
  
has_key(Key, XHash = #xhash{capacity=Capacity,data=Data}) ->
  ?debug("has_key(~p, XHash)", [Key]),
  UnrevHash = lib_misc:hash(Key),
  KeyHash = lib_misc:reverse_bits(UnrevHash),
  Bucket = UnrevHash rem Capacity,
  {_, Pointer} = read_bucket(Bucket, XHash),
  if
    Pointer == 0 -> {ok, false};
    true ->
      case find_node(Pointer, Key, KeyHash, XHash) of
        not_found -> {ok, false};
        {NewPointer, Header} -> {ok, true}
      end
  end.
  
delete(Key, #xhash{}) ->
  ok.
  
fold(Fun, XHash = #xhash{head=Head}, AccIn) when is_function(Fun) ->
  int_fold(Fun, Head, XHash, AccIn).
  
%%====================================================================
%% Internal functions
%%====================================================================
double_index(XHash = #xhash{index=Index,data=Data,capacity=Capacity}) ->
  GrowSize = Capacity * 64,
  {ok, Position} = file:position(Index, eof),
  file:write(Index, <<0:GrowSize>>),
  XHash#xhash{capacity=Capacity * 2}.

dump(XHash = #xhash{head=Head,size=Size,capacity=Capacity}) ->
  IndexList = dump_index(XHash),
  DataList = dump_data(XHash),
  % {Head, IndexList, DataList}.
  {[{head,Head},{size,Size},{capacity,Capacity}], interleave_dump(IndexList, DataList)}.
  
dump_index(XHash = #xhash{index=Index}) ->
  Pointer = read_pointer(0, XHash),
  dump_index(Pointer, 0, [], XHash).
  
dump_index(Pointer, Bucket, List, XHash = #xhash{capacity=Capacity}) when Bucket == Capacity - 1 ->
  lists:reverse([{index, Bucket,Pointer,lib_misc:reverse_bits(Bucket)}|List]);  

dump_index(Pointer, Bucket, List, XHash = #xhash{index=Index,capacity=Capacity}) ->
  case next_bucket(Bucket, Capacity, XHash) of
    {NextPointer, NextBucket} -> dump_index(NextPointer, NextBucket, [{index,Bucket, Pointer,lib_misc:reverse_bits(Bucket)}|List], XHash);
    eof -> lists:reverse([{index,Bucket,Pointer,lib_misc:reverse_bits(Bucket)}|List])
  end.
  
dump_data(XHash = #xhash{head=0}) -> [];
  
dump_data(XHash = #xhash{data=Data,head=Head}) ->
  Header = read_node_header(Head, XHash),
  dump_data(Head, Header, [], XHash).
  
dump_data(LastPointer, LastHeader = #node_header{nextptr=0}, List, XHash) ->
  lists:reverse([{node,LastHeader#node_header.keyhash,LastPointer,lib_misc:reverse_bits(LastHeader#node_header.keyhash)}|List]);
  
dump_data(LastPtr, LastHeader = #node_header{nextptr=NextPointer}, List, XHash = #xhash{data=Data}) ->
  Header = read_node_header(NextPointer, XHash),
  dump_data(NextPointer, Header, [{node,LastHeader#node_header.keyhash,LastPtr,lib_misc:reverse_bits(LastHeader#node_header.keyhash)}|List], XHash).

interleave_dump(IndexList, DataList) ->
  interleave_dump([], IndexList, DataList).
  
interleave_dump(Results, [], DataList) ->
  lists:reverse(lists:reverse(DataList) ++ Results);
  
interleave_dump(Results, IndexList, []) ->
  lists:reverse([IndexList|Results]);
  
interleave_dump(Results, IndexList, [{node,KH,Ptr,RevKey}|DataList]) ->
  {LTE, GT} = lists:partition(fun({index,Bucket,Ptr,Hash}) -> Hash =< KH end, IndexList),
  interleave_dump([{node,KH,Ptr,RevKey},LTE|Results], GT, DataList).

next_bucket(Bucket, Capacity, XHash) when Bucket == Capacity - 1 ->
  eof;

next_bucket(Bucket, Capacity, XHash) ->
  case read_pointer(Bucket+1, XHash) of
    0 -> next_bucket(Bucket+1, Capacity, XHash);
    Pointer -> {Pointer, Bucket+1}
  end.

int_fold(Fun, 0, XHash, AccIn) -> AccIn;

int_fold(Fun, Pointer, XHash = #xhash{data=Data}, AccIn) ->
  NodeHeader = read_node_header(Pointer, XHash),
  if 
    NodeHeader#node_header.keysize == 0 -> 
      int_fold(Fun, NodeHeader#node_header.nextptr, XHash, AccIn);
    true ->
      {Key, Context, Values} = read_node_data(Pointer, NodeHeader, Data),
      AccOut = Fun({Key, Context, Values}, AccIn),
      int_fold(Fun, NodeHeader#node_header.nextptr, XHash, AccOut)
  end.

write_node(Bucket, Header, KeyBin, DataBin, XHash = #xhash{index=Index,data=Data}) ->
  ?debug("write_node(~p, ~p, KeyBin, DataBin, XHash)", [Bucket, Header]),
  {Pointer, StartHeader, IsParent} = find_start(Bucket, XHash),
  {NewPointer, XHash1, Depth, InsertToParent} = case insert_node(Pointer, StartHeader, Header, KeyBin, DataBin, XHash, 0) of
    insert_to_parent ->
      {NP, XH} = write_node(?PARENT(Bucket), Header, KeyBin, DataBin, XHash),
      {NP, XH, 0, true};
    {P, X, D} -> {P, X, D, false}
  end,
  ?debug("write_node decision Depth: ~w IsParent ~w InsertToParent ~w", [Depth, IsParent, InsertToParent]),
  {_, XHash2} = if
    % (Depth > 0) and 
    InsertToParent -> walk_to_initialize(NewPointer, Bucket, XHash1);
    Depth == 0 -> walk_to_initialize(NewPointer, Bucket, XHash1);
    IsParent -> walk_to_initialize(Pointer, Bucket, XHash1);
    true -> {ok, XHash1}
  end,
  {NewPointer, XHash2}.
  
initialize_bucket2(0, XHash = #xhash{index=Index,data=Data,head=0}) ->
  ?debugmsg("initialize_bucket2(0, #xhash{head=0})"),
  {0, XHash};
  
initialize_bucket2(0, XHash = #xhash{head=Head}) ->
  ?debugmsg("initialize_bucket2(0, XHash)"),
  Pointer = read_pointer(0, XHash),
  {Pointer, XHash};

initialize_bucket2(Bucket, XHash = #xhash{index=Index,data=Data}) ->
  Parent = ?PARENT(Bucket),
  ?debug("initialize_bucket2(~p, XHash) ~p", [Bucket, Parent]),
  case read_pointer(Bucket, XHash) of
    0 -> 
      {Ptr, XHash1} = initialize_bucket2(Parent, XHash),
      walk_to_initialize(Ptr, Bucket, XHash1);
    Ptr -> {Ptr, XHash}
  end.

walk_to_initialize(0, Bucket, XHash = #xhash{}) ->
  {0, XHash};

walk_to_initialize(Ptr, Bucket, XHash = #xhash{index=Index,data=Data}) ->
  ?debug("walk_to_initialize(~w, ~w, XHash)", [Ptr, Bucket]),
  Header = read_node_header(Ptr, XHash),
  walk_to_initialize(Ptr, Header, Bucket, XHash).
  
walk_to_initialize(0, Header, Bucket, XHash) ->
  {0, XHash};
  
walk_to_initialize(_, _, 0, XHash) ->
  {0, XHash};
  
walk_to_initialize(Ptr, Header = #node_header{nextptr=0}, Bucket, XHash) ->
  {0, XHash};
  
walk_to_initialize(Pointer, Header = #node_header{nextptr=Ptr,keyhash=KeyHash}, Bucket, XHash = #xhash{index=Index,data=Data,capacity=Capacity})  ->
  UnrevHash = lib_misc:reverse_bits(KeyHash),
  BucketHash = lib_misc:reverse_bits(Bucket),
  ?debug("walk_to_initialize(~w, ~w, ~w, XHash)", [Pointer, Header, Bucket]),
  if
    
    UnrevHash rem Capacity == Bucket -> {Pointer, write_bucket(Bucket, Pointer, XHash)};
    BucketHash < KeyHash -> {Pointer, write_bucket(Bucket, Pointer, XHash)};
    true ->
      NextHeader = read_node_header(Ptr, XHash),
      walk_to_initialize(Ptr, NextHeader, Bucket, XHash)
  end.
  
initialize_bucket(0, XHash = #xhash{index=Index,data=Data,head=0}) ->
  {ok, Pointer, XHash1} = write_node_header(#node_header{}, eof, XHash),
  XHash2 = write_bucket(0, Pointer, XHash1),
  XHash3 = XHash2#xhash{head=Pointer},
  write_head_pointer(Pointer, Data),
  {Pointer, XHash3};

initialize_bucket(0, XHash = #xhash{index=Index,data=Data}) ->
  {read_pointer(0, XHash), XHash};

initialize_bucket(Bucket, XHash = #xhash{index=Index,data=Data}) ->
  Parent = ?PARENT(Bucket),
  ?debug("initialize_bucket(~p, XHash) ~p", [Bucket, Parent]),
  {ParentPointer, XHash1} = case read_pointer(?PARENT(Bucket), XHash) of
    0 -> initialize_bucket(?PARENT(Bucket), XHash);
    Ptr -> {Ptr, XHash}
  end,
  ?debug("inserting dummy node keyhash = ~p", [lib_misc:reverse_bits(Bucket)]),
  {NodePointer, XHash2, _} = insert_node(ParentPointer, #node_header{keyhash=lib_misc:reverse_bits(Bucket)}, <<"">>, <<"">>, XHash1),
  ?debug("nodepointer ~p", [NodePointer]),
  XHash3 = write_bucket(Bucket, NodePointer, XHash2),
  {NodePointer, XHash3}.

% the list is empty
insert_node(Pointer, Header, KeyBin, DataBin, XHash = #xhash{data=Data,head=0}) ->
  ?debug("insert_node(~w, ~w, KeyBin, DataBin, ~w) -> empty list", [Pointer, Header, XHash]),
  {ok, NewPointer, XHash1} = write_node_header(Header, eof, XHash),
  ok = write_node_data(KeyBin, DataBin, eof, Data),
  XHash2 = increment_size(Header, XHash1),
  write_head_pointer(NewPointer,Data),
  XHash3 = write_bucket(0, NewPointer, XHash2),
  {NewPointer, XHash3#xhash{head=NewPointer}, 0};

insert_node(0, Header, KeyBin, DataBin, XHash) ->
  insert_node(0, nil, Header, KeyBin, DataBin, XHash, 0);

insert_node(Pointer, Header, KeyBin, DataBin, XHash = #xhash{index=Index,data=Data}) ->
  ReadHeader = read_node_header(Pointer, XHash),
  insert_node(Pointer, ReadHeader, Header, KeyBin, DataBin, XHash, 0).
  
insert_node(Pointer, LastHeader = #node_header{keyhash=LastKH}, Header = #node_header{keyhash=KeyHash}, KeyBin, DataBin, XHash = #xhash{index=Index,data=Data,size=Size,head=Pointer}, Depth) when KeyHash =< LastKH ->
  ?debug("insert_node(~w, ~w, ~w, KeyBin, DataBin, XHash, ~w) -> start of the list", [Pointer, LastHeader, Header, Depth]),
  {ok, NewPointer, XHash1} = write_node_header(Header#node_header{nextptr=Pointer}, eof, XHash),
  ok = write_node_data(KeyBin, DataBin, eof, Data),
  write_head_pointer(NewPointer, Data),
  XHash2 = write_bucket(0, NewPointer, XHash1),
  {NewPointer, increment_size(Header, XHash2#xhash{head=NewPointer}), Depth};
  
insert_node(0, nil, Header = #node_header{keyhash=KeyHash}, KeyBin, DataBin, XHash = #xhash{index=Index,data=Data,size=Size,head=Pointer}, Depth) ->
  ?debug("insert_node(~w, ~w, ~w, KeyBin, DataBin, XHash, ~w) -> start of the list", [0, nil, Header, Depth]),
  {ok, NewPointer, XHash1} = write_node_header(Header#node_header{nextptr=Pointer}, eof, XHash),
  ok = write_node_data(KeyBin, DataBin, eof, Data),
  write_head_pointer(NewPointer, Data),
  XHash2 = write_bucket(0, NewPointer, XHash1),
  {NewPointer, increment_size(Header, XHash2#xhash{head=NewPointer}), Depth};
  
insert_node(Pointer, LastHeader = #node_header{keyhash=LastKH}, Header = #node_header{keyhash=KeyHash}, KeyBin, DataBin, XHash = #xhash{index=Index,data=Data}, Depth) when KeyHash =< LastKH ->
  ?debug("insert_node(~w, ~w, ~w, KeyBin, DataBin, XHash) -> before bucket", [Pointer, LastHeader, Header]),
  insert_to_parent;
  
insert_node(Pointer, LastHeader = #node_header{nextptr=0}, Header = #node_header{}, KeyBin, DataBin, XHash = #xhash{index=Index,data=Data,size=Size}, Depth) ->
  ?debug("insert_node(~w, ~w, ~w, KeyBin, DataBin, XHash, ~w) -> end of the list", [Pointer, LastHeader, Header, Depth]),
  {ok, NewPointer, XHash1} = write_node_header(Header#node_header{}, eof, XHash),
  ok = write_node_data(KeyBin, DataBin, eof, Data),
  {ok, _, XHash2} = write_node_header(LastHeader#node_header{nextptr=NewPointer}, Pointer, XHash1),
  XHash3 = increment_size(Header, XHash2),
  {NewPointer, XHash3, Depth};
  
insert_node(Pointer, LastHeader, Header = #node_header{keyhash=KeyHash}, KeyBin, DataBin, XHash = #xhash{index=Index,data=Data,size=Size,capacity=Capacity}, Depth) ->
  ?debug("insert_node(~w, ~w, ~w, KeyBin, DataBin, XHash, ~w)", [Pointer, LastHeader, Header, Depth]),
  NextHeader = read_node_header(LastHeader#node_header.nextptr, XHash),
  UnrevHash = lib_misc:reverse_bits(LastHeader#node_header.keyhash),
  if
    KeyHash =< NextHeader#node_header.keyhash ->
      {ok, NewPointer, XHash1} = write_node_header(Header#node_header{nextptr=LastHeader#node_header.nextptr}, eof, XHash),
      ok = write_node_data(KeyBin, DataBin, eof, Data),
      {ok, _, XHash2} = write_node_header(LastHeader#node_header{nextptr=NewPointer}, Pointer, XHash1),
      % {ok, _} = write_node_header(NextHeader#node_header{lastptr=NewPointer}, Pointer, Data),
      XHash3 = increment_size(Header, XHash2),
      {NewPointer, XHash3, Depth+1};
    true -> insert_node(LastHeader#node_header.nextptr, NextHeader, Header, KeyBin, DataBin, XHash, Depth+1)
  end.

read_node_data(Pointer, #node_header{keysize=KeySize,datasize=DataSize}, Data) ->
  {ok, <<KeyBin:KeySize/binary, DataBin:DataSize/binary>>} = file:pread(Data, Pointer+18, KeySize+DataSize),
  Key = binary_to_list(KeyBin),
  {Context, Values} = binary_to_term(DataBin),
  {Key, Context, Values}.

find_node(0, Key, KeyHash, _) ->
  not_found;

find_node(Pointer, Key, KeyHash, XHash) ->
  Header = read_node_header(Pointer, XHash),
  ?debug("find_node(~p, ~p, ~p, Data, _) -> ~p", [Pointer, Key, KeyHash, Header#node_header.keyhash]),
  if
    KeyHash == Header#node_header.keyhash -> {Pointer, Header};
    KeyHash > Header#node_header.keyhash -> find_node(Header#node_header.nextptr, Key, KeyHash, XHash);
    true -> not_found
  end.

write_node_header(Header, eof, XHash = #xhash{data=Data}) ->
  {ok, Pointer} = file:position(Data, eof),
  write_node_header(Header, Pointer, XHash);

write_node_header(Header = #node_header{keyhash=KeyHash,nextptr=NextPointer,keysize=KeySize,datasize=DataSize}, Pointer, XHash = #xhash{data=Data}) ->
  XHash1 = node_cache_set(Pointer, Header, XHash),
  ?debug("write_node_header(~p,~p,Data)", [Header, Pointer]),
  ok = file:pwrite(Data, Pointer, <<KeyHash:32, NextPointer:64, KeySize:16, DataSize:32>>),
  {ok, Pointer, XHash1}.
  
write_node_data(KeyBin, DataBin, Pointer, Data) ->
  file:position(Data, Pointer),
  ok = file:write(Data, [KeyBin, DataBin]).

read_node_header(0, Data) ->
  nil;

read_node_header(Pointer, XHash = #xhash{data=Data}) ->
  Header = case node_cache_get(Pointer, XHash) of
    not_found -> 
      {ok, <<KeyHash:32/integer, NextPointer:64/integer, KeySize:16/integer, DataSize:32/integer>>} = file:pread(Data, Pointer, 18),
      #node_header{keyhash=KeyHash,nextptr=NextPointer,keysize=KeySize,datasize=DataSize};
    Val -> Val
  end,
  ?debug("read_node_header(~p, Data) -> ~p", [Pointer, Header]),
  Header.

find_start(0, XHash = #xhash{data=Data}) ->
  ?debugmsg("find_start(0, XHash)"),
  Pointer = read_pointer(0, XHash),
  Header = read_node_header(Pointer, XHash),
  {Pointer, Header, false};
  
find_start(Bucket, XHash = #xhash{}) ->
  find_start(Bucket, XHash, false).
  
find_start(Bucket, XHash = #xhash{data=Data}, IsParent) ->
  ?debug("find_start(~w, XHash)", [Bucket]),
  case read_pointer(Bucket, XHash) of
    0 -> find_start(?PARENT(Bucket), XHash, true);
    Pointer -> {Pointer, read_node_header(Pointer,XHash), IsParent}
  end.

read_bucket(0, XHash) ->
  ?debugmsg("read_bucket 0"),
  {0, read_pointer(0, XHash)};

read_bucket(Bucket, XHash) ->
  Parent = ?PARENT(Bucket),
  ?debug("read_bucket(~p, Index) -> ~p", [Bucket, Parent]),
  Pointer = read_pointer(Bucket, XHash),
  if
    Pointer == 0 -> read_bucket(Parent, XHash);
    true -> {Bucket, Pointer}
  end.
  
write_bucket(Bucket, Pointer, XHash = #xhash{index=Index}) ->
  XHash1 = index_cache_set(Bucket, Pointer, XHash),
  ?debug("write_bucket(~p, ~p, Index) -> ~p", [Bucket, Pointer, 8*Bucket + ?INDEX_HEADER_SIZE]),
  file:pwrite(Index, 8*Bucket + ?INDEX_HEADER_SIZE, <<Pointer:64>>),
  XHash1.
  
read_pointer(Bucket, XHash = #xhash{index=Index}) ->
  case index_cache_get(Bucket, XHash) of
    not_found -> 
      Loc = 8 * Bucket + ?INDEX_HEADER_SIZE,
      {ok, <<Pointer:64/integer>>} = file:pread(Index, Loc, 8),
      ?debug("read_pointer(~p, Index) -> ~p", [Bucket, Pointer]),
      Pointer;
    Pointer -> Pointer
  end.
    
node_cache_get(Pointer, XHash = #xhash{node_cache=Cache}) ->
  case memcache:get(Cache, integer_to_list(Pointer)) of
    {ok, Header} -> Header;
    error -> not_found
  end.
  
node_cache_set(Pointer, Header, XHash = #xhash{node_cache=Cache}) ->
  memcache:set(Cache, integer_to_list(Pointer), Header),
  XHash.
    
index_cache_get(Bucket, XHash = #xhash{idx_cache=Cache,index=Index}) ->
  N = (Bucket * 8) div ?CHUNK_SIZE,
  BytePos = (Bucket * 8) rem ?CHUNK_SIZE,
  case memcache:get(Cache, integer_to_list(N)) of
    {ok, Binary} when BytePos >= byte_size(Binary) -> 
      ?debug("index_cache_get(~w, XHash) N ~w BytePos ~w cache miss", [Bucket, N, BytePos]),
      not_found;
    {ok, <<_:BytePos/binary, Pointer:64/integer, _/binary>>}  ->
      ?debug("index_cache_get(~w, XHash) N ~w BytePos ~w Pointer ~w cache hit", [Bucket, N, BytePos, Pointer]),
      Pointer;
    _ -> 
      ?debug("index_cache_get(~w, XHash) N ~w BytePos ~w cache miss", [Bucket, N, BytePos]),
      not_found
  end.

index_cache_set(Bucket, Pointer, XHash = #xhash{idx_cache=Cache,index=Index}) ->
  N = (Bucket * 8) div ?CHUNK_SIZE,
  Chunk = N * ?CHUNK_SIZE,
  BytePos = (Bucket * 8) rem ?CHUNK_SIZE,
  ?debug("index_cache_set(~w, ~w, XHash) N ~w Chunk ~w BytePos ~w", [Bucket, Pointer, N, Chunk, BytePos]),
  NewBinary = case memcache:get(Cache, integer_to_list(N)) of
    {ok, Binary} when BytePos >= byte_size(Binary) ->
      NewPos = (BytePos - byte_size(Binary))*8,
      <<Binary/binary, 0:NewPos/integer, Pointer:64/integer>>;
    {ok, <<Before:BytePos/binary, _:8/binary, After/binary>>} -> 
      <<Before/binary, Pointer:64/integer, After/binary>>;
    _ ->
      case file:pread(Index, Chunk + ?INDEX_HEADER_SIZE, ?CHUNK_SIZE) of
        {ok, <<Before:BytePos/binary, _:8/binary, After/binary>>} -> <<Before/binary, Pointer:64/integer, After/binary>>;
        eof -> eof
      end
  end,
  if
    NewBinary == eof -> Cache;
    true -> memcache:set(Cache, integer_to_list(N), NewBinary)
  end,
  XHash.

initialize_or_verify(Hash = #xhash{data=Data,index=Index}) ->
  case {read_data_header(Data), read_index_header(Index)} of
    {{ok, <<"XD">>, ?VERSION, Size, Head}, {ok, <<"XI">>, ?VERSION, Capacity}} -> {ok, Hash#xhash{size=Size,head=Head,capacity=Capacity}};
    {eof, eof} -> initialize(Hash);
    {FailureA, FailureB} -> {error, io_lib:format("could not initialize: ~w", [{FailureA, FailureB}])}
  end.
  
read_data_header(File) ->
  case file:pread(File, 0, 16) of
    {ok, <<Id:2/binary, Version:16/integer, Size:32/integer, Head:64/integer>>} -> {ok, Id, Version, Size, Head};
    {error, Reason} -> {error, Reason};
    eof -> eof
  end.
  
read_index_header(File) ->
  case file:pread(File, 0, 8) of
    {ok, <<Id:2/binary, Version:16/integer, Capacity:32>>} -> {ok, Id, Version, Capacity};
    {error, Reason} -> {error, Reason};
    eof -> eof 
  end.
  
increment_size(#node_header{keysize=0}, XHash) ->
  XHash;
  
increment_size(_, XHash = #xhash{size=Size, data=Data}) ->
  write_size(Size+1, Data),
  XHash#xhash{size=Size+1}.
  
write_capacity(Capacity, Index) ->
  file:pwrite(Index, 4, <<Capacity:32>>).
  
write_size(Size, Data) ->
  file:pwrite(Data, 4, <<Size:32>>).
  
write_head_pointer(Pointer, Data) ->
  ?debug("write_head_pointer(~w, Data)", [Pointer]),
  file:pwrite(Data, 8, <<Pointer:64>>).
  
initialize(Hash = #xhash{data=Data,index=Index}) ->
  Size = 0,
  Head = 0,
  Capacity = 1,
  TableSize = Capacity * 64,
  case file:pwrite(Data, 0, <<"XD", ?VERSION:16, Size:32, Head:64, 0:256>>) of
    ok ->
      case file:pwrite(Index, 0, <<"XI", ?VERSION:16, Capacity:32, 0:256, 0:TableSize>>) of
        ok -> {ok, init_node_cache(init_index_cache(Hash#xhash{head=Head,capacity=Capacity,size=Size}))};
        Failure -> Failure
      end;
    Failure -> Failure
  end.

init_index_cache(XHash = #xhash{capacity=Capacity,index=Index}) ->
  {ok, Memcache} = memcache:start(8),
  lists:foldl(fun(N, D) ->
      Chunk = N * ?CHUNK_SIZE,
      if
        Chunk >= Capacity -> D;
        true ->
          case file:pread(Index, Chunk + ?INDEX_HEADER_SIZE, ?CHUNK_SIZE) of
            {ok, Binary} -> memcache:set(Memcache, integer_to_list(N), Binary);
            eof -> D
          end
      end
    end, Memcache, lists:seq(0,?MAX_CHUNKS-1)),
  XHash#xhash{idx_cache=Memcache}.
  
init_node_cache(XHash) ->
  {ok, Memcache} = memcache:start(8),
  XHash#xhash{node_cache=Memcache}.