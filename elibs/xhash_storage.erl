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
-define(PARENT(Bucket), if Bucket > 0 -> Bucket bxor (1 bsl round(math:log(Bucket)/math:log(2))); true -> 0 end).
-record(xhash, {data,index,head,capacity,size}).
-record(node, {key=nil, data=nil, node_header}).
-record(node_header, {keyhash=0,keysize=0,datasize=0,pointer=0}).


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
  DataFileName = lists:concat([Directory, "/xhash.xd"]),
  IndexFileName = lists:concat([Directory, "/xhash.xi"]),
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
      KeyHash = lib_misc:reverse_bits(lib_misc:hash(Key)),
      BucketIndex = KeyHash rem Capacity,
      Pointer = read_bucket(BucketIndex, Index),
      case find_node(Pointer, Key, KeyHash, Data, Head) of
        not_found -> {ok, not_found};
        Header -> {ok, read_node_data(Header, Data)}
      end
  end.
  
put(Key, Context, Values, XHash = #xhash{capacity=Capacity,size=Size,data=Data,index=Index,head=Head}) ->
  KeyHash = lib_misc:reverse_bits(lib_misc:hash(Key)),
  Bucket = KeyHash rem Capacity,
  KeyBin = list_to_binary(Key),
  DataBin = terms_to_binary({Context, Values}),
  Header = #node_header{keyhash=KeyHash,keysize=byte_size(KeyBin),datasize=byte_size(DataBin)},
  NewHash = write_node(Bucket, Header, KeyBin, DataBin, XHash).
  
has_key(Key, #xhash{}) ->
  ok.
  
delete(Key, #xhash{}) ->
  ok.
  
fold(Fun, XHash = #xhash{head=Head}, AccIn) when is_function(Fun) ->
  int_fold(Fun, Head, XHash, AccIn).
  
%%====================================================================
%% Internal functions
%%====================================================================
int_fold(Fun, Pointer, XHash = #xhash{data=Data}, AccIn) ->
  NodeHeader = read_node_header(Pointer, Data),
  if 
    NodeHeader#node_header.keysize == 0 -> 
      int_fold(Fun, NodeHeader#node_header.pointer, XHash, AccIn);
    true ->
      {Key, Context, Values} = read_node_data(NodeHeader, Data),
      AccOut = Fun({Key, Context, Values}, AccIn),
      int_fold(Fun, NodeHeader#node_header.pointer, XHash, AccOut)
  end.

write_node(Bucket, Header, KeyBin, DataBin, XHash = #xhash{index=Index,data=Data}) ->
  {Pointer, XHash1} = case read_pointer(Bucket, Index) of
    0 -> initialize_bucket(Bucket, XHash);
    Pointer -> {Pointer, XHash}
  end,
  XHash2 = insert_node(Pointer, Header, KeyBin, DataBin, XHash1).

initialize_bucket(0, XHash = #xhash{index=Index,data=Data}) ->
  Pointer = write_node(eof, #node_header{}, <<"">>, <<"">>, Data),
  write_pointer(0, Pointer, Index),
  Xhash1 = XHash#xhash{head=Pointer},
  write_headers(XHash1),
  {Pointer, XHash1};

initialize_bucket(Bucket, XHash = #xhash{index=Index,data=Data}) ->
  {ParentPointer, XHash1} = case read_pointer(?PARENT(Bucket), Index) of
    0 -> initialize_bucket(?PARENT(Bucket), Index);
    Ptr -> {Ptr, XHash}
  end,
  {NodePointer, XHash2} = insert_node(ParentPointer, #node_header{keyhash=lib_misc:reversebits(Bucket)}, <<"">>, <<"">>, XHash1),
  write_pointer(Bucket, NodePointer, Index),
  {NodePointer, XHash2}.

insert_node(Pointer, Header, KeyBin, DataBin, XHash = #xhash{index=Index,data=Data}) ->
  ReadHeader = read_node_header(Pointer, Data),
  insert_node(Pointer, ReadHeader, Header, KeyBin, DataBin, XHash).
  
insert_node(Pointer, LastHeader, Header = #node_header{keyhash=KeyHash}, KeyBin, DataBin, XHash = #xhash{index=Index,data=Data,size=Size}) ->
  NextHeader = read_node_header(LastHeader#node_header.pointer, Data),
  if
    KeyHash =< NextHeader#node_header.keyhash ->
      {ok, NewPointer} = write_node_header(Header#node_header{pointer=LastHeader#node_header.pointer}, eof, Data),
      ok = write_node_data(KeyBin, DataBin, eof, Data),
      {ok, _} = write_node_header(LastHeader#node_header{pointer=NewPointer}, Pointer, Data),
      write_size(Size+1, Data),
      XHash#xhash{size=Size+1};
    true -> insert_node(LastHeader#node_header.pointer, NextHeader, Header, KeyBin, DataBin, XHash)
  end.

read_node_data(#node_header{pointer=Pointer,keysize=KeySize,datasize=DataSize}, Data) ->
  {ok, <<KeyBin:KeySize/binary, DataBin:DataSize/binary>>} = file:pread(Data, Pointer+18, KeySize+DataSize),
  Key = binary_to_list(KeyBin),
  {Context, Values} = binary_to_term(DataBin),
  {Key, Context, Values}.

find_node(0, Key, KeyHash, Data, Head) ->
  find_node(Head, Key, KeyHash, Data, Head);

find_node(Pointer, Key, KeyHash, Data, _) ->
  Header = read_node_header(Pointer, Data),
  if
    KeyHash == Header#node_header.keyhash -> Header;
    KeyHash > Header#node_header.keyhash -> read_node_header(Header#node_header.pointer, Data);
    true -> not_found
  end.

write_node_header(Header, eof, Data) ->
  {ok, Pointer} = file:position(Data, eof),
  write_node_header(Header, Pointer, Data);

write_node_header(Header = #node_header{keyhash=KeyHash,pointer=NextPointer,keysize=KeySize,datasize=DataSize}, Pointer, Data) ->
  ok = file:pwrite(Data, Pointer, <<KeyHash:32, NextPointer:64, KeySize:16, DataSize:32>>),
  {ok, Pointer}.
  
write_node_data(KeyBin, DataBin, Pointer, Data) ->
  file:position(Data, Pointer),
  ok = file:write(Data, [KeyBin, DataBin]).

read_node_header(Pointer, Data) ->
  {ok, <<KeyHash:32/integer, NextPointer:64/integer, KeySize:16/integer, DataSize:32/integer>>} = file:pread(Data, Pointer, 18),
  #node_header{keyhash=KeyHash,pointer=NextPointer,keysize=KeySize,datasize=DataSize}.

read_bucket(0, Index) ->
  read_pointer(0, Index);

read_bucket(Bucket, Index) ->
  Parent = ?PARENT(Bucket),
  Pointer = read_pointer(Bucket, Index),
  if
    Pointer == 0 -> read_bucket(Parent);
    true -> Pointer
  end.
  
write_bucket(Bucket, Pointer, Index) ->
  file:pwrite(Index, 8*Bucket + ?INDEX_HEADER_SIZE, <<Pointer:64>>).
  
write_pointer(Bucket, Index) ->
  file:pwrite(Index, 8 * BucketIndex + ?INDEX_HEADER_SIZE).
  
read_pointer(Bucket, Index) ->
  {ok, <<Pointer:64/integer>>} = file:pread(Index, 8 * BucketIndex + ?INDEX_HEADER_SIZE),
  Pointer.

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
  
write_size(Size, Data) ->
  file:pwrite(Data, 4, <<Size:32>>).
  
initialize(Hash = #xhash{data=Data,index=Index}) ->
  Size = 0,
  Head = 48,
  Capacity = 1024,
  TableSize = Capacity * 64,
  case file:pwrite(Data, 0, <<"XD", ?VERSION:16, Size:32, Head:64, 0:256>>) of
    ok ->
      case file:pwrite(Index, 0, <<"XI", ?VERSION:16, Capacity:32, 0:256, 0:TableSize>>) of
        ok -> {ok, Hash#xhash{head=Head,capacity=Capacity,size=Size}};
        Failure -> Failure
      end;
    Failure -> Failure
  end.
