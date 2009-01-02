%%%-------------------------------------------------------------------
%%% File:      xhash_storage.erl
%%% @author    Cliff Moon <cliff@powerset.com> []
%%% @copyright 2008 Cliff Moon
%%% @doc  
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
-record(xhash, {data,index,head,capacity,size}).
-record(node, {key=nil, data=nil, node_header}).
-record(node_header, {keyhash,keysize,datasize,pointer}).

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
  Pointer = read_bucket(Bucket, Index),
  KeyBin = list_to_binary(Key),
  DataBin = terms_to_binary({Context, Values}),
  Header = #node_header{keyhash=KeyHash,keysize=byte_size(KeyBin),datasize=byte_size(DataBin)},
  if
    Pointer == 0 ->
    true -> 
  end
  
has_key(Key, #xhash{}) ->
  ok.
  
delete(Key, #xhash{}) ->
  ok.
  
fold(Fun, #xhash{}, AccIn) when is_function(Fun) ->
  ok.
  
%%====================================================================
%% Internal functions
%%====================================================================
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
  
initialize(Hash = #xhash{data=Data,index=Index}) ->
  Size = 0,
  Head = 0,
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


