%%%-------------------------------------------------------------------
%%% File:      dmerkle.erl
%%% @author    Cliff Moon <> []
%%% @copyright 2008 Cliff Moon
%%% @doc  
%%%
%%% @end  
%%%
%%% @since 2008-08-03 by Cliff Moon
%%%-------------------------------------------------------------------
-module(dmerkle).
-author('cliff moon').

-record(dmerkle, {file, block, root, d}).

-record(node, {m=0, keys=[], children=[], offset=eof}).
-record(leaf, {m=0, values=[], offset=eof}).


%% API
-export([open/1, open/2, update/3, delete/2, leaf_size/1, key_diff/2, close/1]).

-ifdef(TEST).
-include("etest/dmerkle_test.erl").
-endif.

%%====================================================================
%% Internal API
%%====================================================================
%%--------------------------------------------------------------------
%% @spec 
%% @doc
%% @end 
%%--------------------------------------------------------------------

open(FileName) ->
  open(FileName, undefined).

open(FileName, BlockSize) -> 
  {ok, File} = block_server:start_link(FileName, BlockSize),
  FinalBlockSize = case block_server:read_block(File, 0, 4) of
    {ok, <<ReadBlockSize:32>>} -> 
      D = d_from_blocksize(ReadBlockSize),
      ReadBlockSize;
    eof -> 
      D = d_from_blocksize(BlockSize),
      ModBlockSize = blocksize_from_d(D),
      block_server:write_block(File, 0, <<ModBlockSize:32>>),
      ModBlockSize
  end,
  Root = create_or_read_root(File, FinalBlockSize),
  #dmerkle{file=File,block=FinalBlockSize,root=Root,d=D}.

update(Key, Value, Tree = #dmerkle{file=File,block=BlockSize,d=D,root=Root}) ->
  M = m(Root),
  if
    M >= D-1 -> %allocate new root, move old root and split
      NewRoot = #node{},
      FinalRoot = split_child(NewRoot, empty, Root, Tree),
      update_root_pointer(File, FinalRoot),
      Tree#dmerkle{root=update(hash(Key), Key, Value, FinalRoot, Tree)};
    true -> Tree#dmerkle{root=update(hash(Key), Key, Value, Root, Tree)}
  end.

find(Key, Tree = #dmerkle{root=Root}) ->
  find(hash(Key), Key, Root, Tree).

delete(Key, Merkle) -> noop.

leaf_size(Merkle) -> noop.

key_diff(MerkleA, MerkleB) -> noop.

close(#dmerkle{file=File}) ->
  block_server:stop(File).

%%====================================================================
%% Internal functions
%%====================================================================

find(KeyHash, Key, Node = #node{keys=Keys,children=Children}, Tree = #dmerkle{file=File,block=BlockSize}) ->
  {_, {_,ChildPointer}} = find_child(KeyHash, Keys, Children),
  find(KeyHash, Key, read(File,ChildPointer,BlockSize), Tree);
  
find(KeyHash, Key, Leaf = #leaf{values=Values}, Tree = #dmerkle{file=File,block=BlockSize}) ->
  case lists:keysearch(KeyHash, 1, Values) of
    {value, {KeyHash,_,ValHash}} -> ValHash;
    false -> not_found
  end.

update(KeyHash, Key, Value, Node = #node{children=Children,keys=Keys}, Tree = #dmerkle{d=D,file=File,block=BlockSize}) ->
  {FoundKey, {ChildHash,ChildPointer}} = find_child(KeyHash, Keys, Children),
  Child = read(File,ChildPointer,BlockSize),
  case m(Child) of
    M when M >= D -> update(KeyHash, Key, Value, split_child(Node, FoundKey, Child, Tree), Tree);
    _ -> 
      NewChildHash = hash(update(KeyHash, Key, Value, Child, Tree)),
      write(File, BlockSize, Node#node{children=lists:keyreplace(ChildPointer, 2, Children, {NewChildHash,ChildPointer})})
  end;
  
update(KeyHash, Key, Value, Leaf = #leaf{values=Values}, Tree = #dmerkle{d=D,file=File,block=BlockSize}) ->
  NewValHash = hash(Value),
  case lists:keysearch(KeyHash, 1, Values) of
    {value, {KeyHash,Pointer,ValHash}} ->
      case block_server:read_key(File, Pointer) of
        Key -> 
          error_logger:info_msg("we found the key ~p~n", [Key]),
          write(File, BlockSize, Leaf#leaf{
              values=lists:keyreplace(KeyHash, 1, Values, {KeyHash,Pointer,NewValHash})
            });
        _ ->  %we still need to deal with collision here
          error_logger:info_msg("hash found but no key found, inserting new ~n"),
          {ok, NewPointer} = block_server:write_key(File, eof, Key),
          write(File, BlockSize, Leaf#leaf{
              values=lists:keymerge(1, Values, [{KeyHash,NewPointer,NewValHash}])
            })
      end;
    false ->
      % error_logger:info_msg("no hash or key found, inserting new ~n"),
      {ok, NewPointer} = block_server:write_key(File, eof, Key),
      NewValues = lists:keymerge(1, Values, [{KeyHash,NewPointer,NewValHash}]),
      % error_logger:info_msg("new values: ~p~n", [NewValues]),
      write(File, BlockSize, Leaf#leaf{
          m=length(NewValues),
          values=NewValues
        })
  end.
  
find_child(_, [], [Child]) ->
  {last, Child};
  
find_child(KeyHash, [Key|Keys], [Child|Children]) ->
  if
    KeyHash =< Key -> {Key, Child};
    true -> find_child(KeyHash, Keys, Children)
  end.

split_child(Parent = #node{keys=Keys,children=Children}, ToReplace, Child = #leaf{values=Values,m=M}, #dmerkle{file=File,block=BlockSize}) ->
  {KeyHash, _, _} = lists:nth(M div 2, Values),
  {LeftValues, RightValues} = lists:partition(fun({Hash,_,_}) ->
      Hash =< KeyHash
    end, Values),
  Left = write(File, BlockSize, #leaf{m=length(LeftValues),values=LeftValues}),
  Right = write(File, BlockSize, Child#leaf{m=length(RightValues),values=RightValues}),
  write(File, BlockSize, replace(Parent, ToReplace, Left, Right));
  
split_child(Parent = #node{keys=Keys,children=Children}, ToReplace, Child = #node{m=M,keys=ChildKeys,children=ChildChildren}, #dmerkle{file=File,block=BlockSize}) ->
  KeyHash = lists:nth(M div 2, Keys),
  {LeftKeys, RightKeys} = lists:split((M div 2)-1, ChildKeys),
  {LeftChildren, RightChildren} = lists:split(M div 2, ChildChildren),
  Left = write(File, BlockSize, #node{m=length(LeftKeys),keys=LeftKeys,children=LeftChildren}),
  Right = write(File, BlockSize, Child#node{m=length(RightKeys),keys=RightKeys,children=RightChildren}),
  write(File, BlockSize, replace(Parent, ToReplace, Left, Right)).
  
insert_nonfull() ->
  noop.

replace(Parent = #node{keys=Keys,children=Children}, empty, Left, Right) ->
  KeyHash = last_key(Left),
  Parent#node{
    m=1,
    keys=[KeyHash],
    children=[{hash(Left),offset(Left)},{hash(Right),offset(Right)}]
  };

replace(Parent = #node{keys=Keys,children=Children}, ToReplace, Left, Right) ->
  N = lib_misc:position(ToReplace, Keys),
  error_logger:info_msg("replace toreplace ~p n ~p~n", [ToReplace, N]),
  KeyHash = last_key(Left),
  Parent#node{
    keys = lists:sublist(Keys, N-1) ++ [KeyHash] ++ lists:nthtail(N, Keys),
    children = lists:sublist(Children, N-1) ++ [{hash(Left), offset(Left)}, {hash(Right), offset(Right)}] ++ lists:nthtail(N+1, Children)
  }.

create_or_read_root(File, BlockSize) ->
  case block_server:read_block(File,4,8) of
    eof -> 
      error_logger:info_msg("could not find offset ~n"),
      block_server:write_block(File,4,<<0:64>>), %placeholder
      Root = write(File, BlockSize, #leaf{}),
      update_root_pointer(File, Root),
      Root;
    {ok, Bin} -> 
      <<Offset:64>> = Bin,
      error_logger:info_msg("read offset ~p ~p~n", [Bin, Offset]),
      read(File, Offset, BlockSize)
  end.

update_root_pointer(File, Root) ->
  Offset = offset(Root),
  error_logger:info_msg("writing root offset ~p~n", [Offset]),
  {ok, 4} = block_server:write_block(File,4,<<Offset:64>>).

%node is denoted by a 0
deserialize(<<0:8, Binary/binary>>, Offset) ->
  D = d_from_blocksize(byte_size(Binary) + 1),
  KeyBinSize = D*4,
  ChildBinSize = (D+1)*12,
  <<M:32, KeyBin:KeyBinSize/binary, ChildBin:ChildBinSize/binary>> = Binary,
  Keys = unpack_keys(M, KeyBin),
  Children = unpack_children(M+1, ChildBin),
  #node{m=M,children=Children,keys=Keys,offset=Offset};
  
deserialize(<<1:8, Bin/binary>>, Offset) ->
  D = d_from_blocksize(byte_size(Bin) + 1),
  ValuesBinSize = D*16,
  <<M:32, ValuesBin:ValuesBinSize/binary, _/binary>> = Bin,
  Values = unpack_values(M, ValuesBin),
  #leaf{m=M,values=Values,offset=Offset}.
  
serialize(#node{keys=Keys,children=Children,m=M}, BlockSize) ->
  D = d_from_blocksize(BlockSize),
  KeyBin = pack_keys(Keys, D),
  ChildBin = pack_children(Children, D+1),
  LeftOverBits = (BlockSize - byte_size(KeyBin) - byte_size(ChildBin) - 5)*8,
  <<0:8, M:32, KeyBin/binary, ChildBin/binary, 0:LeftOverBits>>;
  
serialize(#leaf{values=Values,m=M}, BlockSize) ->
  D = d_from_blocksize(BlockSize),
  ValuesBin = pack_values(Values),
  LeftOverBits = (BlockSize - byte_size(ValuesBin) - 5)*8,
  <<1:8, M:32, ValuesBin/binary, 0:LeftOverBits>>.
  
d_from_blocksize(BlockSize) ->
  trunc((BlockSize - 17)/16).
  
blocksize_from_d(D) ->
  trunc(16*D + 17).
  
pack_values(Values) ->
  pack_values(lists:reverse(Values), <<"">>).
  
pack_values([], Bin) -> Bin;
  
pack_values([{KeyHash,KeyPointer,ValHash}|Values], Bin) ->
  pack_values(Values, <<KeyHash:32, KeyPointer:64, ValHash:32, Bin/binary>>).
  
pack_keys(Keys, D) ->
  pack_keys(lists:reverse(Keys), D, <<"">>).
  
pack_keys([], D, Bin) -> 
  BinSize = byte_size(Bin),
  LeftOverBits = D*4*8 - BinSize*8,
  <<Bin/binary, 0:LeftOverBits>>;

pack_keys([KeyHash|Keys], D, Bin) ->
  pack_keys(Keys, D, <<KeyHash:32, Bin/binary>>).
  
pack_children(Children, D) ->
  pack_children(lists:reverse(Children), D, <<"">>).
  
pack_children([], D, Bin) -> 
  BinSize = byte_size(Bin),
  LeftOverBits = D*12*8 - BinSize*8,
  <<Bin/binary, 0:LeftOverBits>>;

pack_children([{ChildHash,ChildPtr}|Children], D, Bin) ->
  pack_children(Children, D, <<ChildHash:32, ChildPtr:64, Bin/binary>>).
  
unpack_keys(M, Bin) ->
  unpack_keys(M, 0, Bin, []).
  
unpack_keys(M, M, Bin, Keys) -> lists:reverse(Keys);
  
unpack_keys(M, N, Bin, Keys) ->
  SkipSize = N*4,
  <<_:SkipSize/binary, KeyHash:32, _/binary>> = Bin,
  unpack_keys(M, N+1, Bin, [KeyHash|Keys]).
  
unpack_children(M, Bin) ->
  unpack_children(M, 0, Bin, []).

unpack_children(M, M, Bin, Children) -> lists:reverse(Children);
  
unpack_children(M, N, Bin, Children) ->
  SkipSize = N*12,
  <<_:SkipSize/binary, ChildHash:32, ChildPtr:64, _/binary>> = Bin,
  unpack_children(M, N+1, Bin, [{ChildHash,ChildPtr}|Children]).
  
unpack_values(M, Bin) ->
  unpack_values(M, 0, Bin, []).
  
unpack_values(M, M, Bin, Values) -> lists:reverse(Values);

unpack_values(M, N, Bin, Values) ->
  SkipSize = N*16,
  <<_:SkipSize/binary, KeyHash:32, KeyPointer:64, ValueHash:32, _/binary>> = Bin,
  unpack_values(M, N+1, Bin, [{KeyHash,KeyPointer,ValueHash}|Values]).
  
  
write(File, BlockSize, Node) ->
  Offset = offset(Node),
  Bin = serialize(Node, BlockSize),
  % error_logger:info_msg("writing out node ~p~n", [Node]),
  
  {ok, NewOffset} = block_server:write_block(File,Offset,Bin),
  if
    NewOffset =< 4 -> error_logger:info_msg("overwrote offset!~n");
    true -> ok
  end,
  % error_logger:info_msg("new offset ~p~n", [NewOffset]),
  offset(Node, NewOffset).
  
read(File, Offset, BlockSize) ->
  {ok, Bin} = block_server:read_block(File, Offset, BlockSize),
  deserialize(Bin,Offset).
  
offset(#leaf{offset=Offset}) -> Offset;
offset(#node{offset=Offset}) -> Offset.

offset(Leaf = #leaf{}, Offset) -> Leaf#leaf{offset=Offset};
offset(Node = #node{}, Offset) -> Node#node{offset=Offset}.

m(#leaf{m=M}) -> M;
m(#node{m=M}) -> M.

m(Leaf = #leaf{}, M) -> Leaf#leaf{m=M};
m(Node = #node{}, M) -> Node#node{m=M}.

last_key(#node{keys=Keys}) -> lists:last(Keys);
last_key(#leaf{values=Values}) ->
  {KeyHash, _, _} = lists:last(Values),
  KeyHash.

hash(V) -> lib_misc:hash(V).