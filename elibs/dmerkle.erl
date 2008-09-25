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

-record(dmerkle, {file, block, root, d, filename}).

-record(node, {m=0, keys=[], children=[], offset=eof}).
-record(leaf, {m=0, values=[], offset=eof}).


%% API
-export([open/1, open/2, equals/2, update/3, delete/2, leaf_size/1, key_diff/2, close/1, scan_for_empty/1, swap_tree/2]).

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
  #dmerkle{file=File,block=FinalBlockSize,root=Root,d=D,filename=FileName}.

update(Key, Value, Tree = #dmerkle{file=File,block=BlockSize,d=D,root=Root}) ->
  M = m(Root),
  if
    M >= D-1 -> %allocate new root, move old root and split
      FinalRoot = split_child(#node{}, empty, Root, Tree),
      update_root_pointer(File, FinalRoot),
      Tree#dmerkle{root=update(hash(Key), Key, Value, FinalRoot, Tree)};
    true -> Tree#dmerkle{root=update(hash(Key), Key, Value, Root, Tree)}
  end.

equals(#dmerkle{root=RootA}, #dmerkle{root=RootB}) ->
  hash(RootA) == hash(RootB).

find(Key, Tree = #dmerkle{root=Root}) ->
  find(hash(Key), Key, Root, Tree).

delete(Key, Merkle) -> Merkle.

leaf_size(Merkle) -> noop.

key_diff(TreeA = #dmerkle{root=RootA}, TreeB = #dmerkle{root=RootB}) ->
  lists:keysort(1, key_diff(RootA, RootB, TreeA, TreeB, [])).

close(#dmerkle{file=File}) ->
  block_server:stop(File).

scan_for_empty(Tree = #dmerkle{root=Root}) ->
  scan_for_empty(Tree, Root).

swap_tree(OldTree = #dmerkle{filename=OldFilename}, NewTree = #dmerkle{filename=NewFilename,block=BlockSize}) ->
  close(OldTree),
  close(NewTree),
  file:copy(block_server:index_name(NewFilename), block_server:index_name(OldFilename)),
  file:copy(block_server:key_name(NewFilename), block_server:key_name(OldFilename)),
  file:delete(block_server:index_name(NewFilename)),
  file:delete(block_server:key_name(NewFilename)),
  open(OldFilename, BlockSize).

%%====================================================================
%% Internal functions
%%====================================================================

scan_for_empty(Tree = #dmerkle{file=File,block=Block}, Node = #node{children=Children,keys=Keys}) ->
  if
    length(Keys) == 0 -> io:format("node was empty: ~p", [Node]);
    true -> noop
  end,
  lists:foreach(fun({_, ChldPtr}) -> scan_for_empty(Tree, read(File, ChldPtr, Block)) end, Children);
  
scan_for_empty(Tree = #dmerkle{file=File,block=Block}, Leaf = #leaf{values=Values}) ->
  if 
    length(Values) == 0 -> io:format("leaf was empty: ~p", [Leaf]);
    true -> noop
  end;
  
scan_for_empty(_, undefined) ->
  io:format("got an undefined!").

key_diff(_LeafA = #leaf{values=ValuesA}, _LeafB = #leaf{values=ValuesB}, 
    #dmerkle{file=FileA}, #dmerkle{file=FileB}, Keys) ->
  % error_logger:info_msg("leaf differences A~p~n B~p~n", [_LeafA, _LeafB]),
  leaf_diff(ValuesA, ValuesB, FileA, FileB, Keys);

key_diff(#node{keys=KeysA, children=ChildrenA},
    #node{keys=KeysB, children=ChildrenB},
    TreeA = #dmerkle{file=FileA}, TreeB = #dmerkle{file=FileB}, Keys) ->
  % error_logger:info_msg("node differences ~n"),
  node_diff(ChildrenA, ChildrenB, TreeA, TreeB, Keys);
  
key_diff(Leaf = #leaf{}, Node = #node{}, TreeA, TreeB, Keys) ->
  % error_logger:info_msg("leaf node differences ~n"),
  leaves(Node, TreeB, leaves(Leaf, TreeA, Keys));
  
key_diff(Node = #node{}, Leaf = #leaf{}, TreeA, TreeB, Keys) ->
  % error_logger:info_msg("node leaf differences ~n"),
  leaves(Leaf, TreeA, leaves(Node, TreeB, Keys)).

node_diff([], [], TreeA, TreeB, Keys) -> Keys;

node_diff([], ChildrenB, TreeA, TreeB = #dmerkle{file=File,block=BlockSize}, Keys) ->
  % error_logger:info_msg("node_diff empty children ~n"),
  lists:foldl(fun({_, Ptr}, Keys) ->
      Child = read(File, Ptr, BlockSize),
      leaves(Child, TreeB, Keys)
    end, Keys, ChildrenB);
    
node_diff(ChildrenA, [], TreeA = #dmerkle{file=File,block=BlockSize}, TreeB, Keys) ->
  % error_logger:info_msg("node_diff children empty ~n"),
  lists:foldl(fun({_, Ptr}, Keys) ->
      Child = read(File, Ptr, BlockSize),
      leaves(Child, TreeA, Keys)
    end, Keys, ChildrenA);
    
node_diff([{Hash,PtrA}|ChildrenA], [{Hash,PtrB}|ChildrenB], TreeA, TreeB, Keys) ->
  % error_logger:info_msg("equal nodes ~n"),
  node_diff(ChildrenA, ChildrenB, TreeA, TreeB, Keys);
  
node_diff([{HashA,PtrA}|ChildrenA], [{HashB,PtrB}|ChildrenB], 
    TreeA=#dmerkle{file=FileA, block=BlockSizeA}, 
    TreeB=#dmerkle{file=FileB,block=BlockSizeB}, Keys) ->
  % error_logger:info_msg("nodes are different ~n"),
  ChildA = read(FileA, PtrA, BlockSizeA),
  ChildB = read(FileB, PtrB, BlockSizeB),
  node_diff(ChildrenA, ChildrenB, TreeA, TreeB, key_diff(ChildA, ChildB, TreeA, TreeB, Keys)).

leaf_diff([], [], _, _, Keys) -> Keys;

leaf_diff([], [{_, Ptr, Val}|ValuesB], FileA, FileB, Keys) ->
  % error_logger:info_msg("leaf_diff empty values ~n"),
  Key = block_server:read_key(FileB, Ptr),
  NewKeys = case lists:keytake(Key, 1, Keys) of
    {value, {Key, Val}, Taken} -> Taken;
    {value, {Key, _}, _} -> Keys;
    false -> [{Key, Val}|Keys]
  end,
  leaf_diff([], ValuesB, FileA, FileB, NewKeys);
  
leaf_diff([{_, Ptr, Val}|ValuesA], [], FileA, FileB, Keys) ->
  % error_logger:info_msg("leaf_diff values empty ~n"),
  Key = block_server:read_key(FileA, Ptr),
  NewKeys = case lists:keytake(Key, 1, Keys) of
    {value, {Key, Val}, Taken} -> Taken;
    {value, {Key, _}, _} -> Keys;
    false -> [{Key, Val}|Keys]
  end,
  leaf_diff(ValuesA, [], FileA, FileB, NewKeys);
  
leaf_diff([{Hash, _, Val}|ValuesA], [{Hash, _, Val}|ValuesB], FileA, FileB, Keys) ->
  % error_logger:info_msg("leaf_diff equals ~n"),
  leaf_diff(ValuesA, ValuesB, FileA, FileB, Keys);
  
leaf_diff([{Hash, PtrA, ValA}|ValuesA], [{Hash, PtrB, ValB}|ValuesB], FileA, FileB, Keys) ->
  % error_logger:info_msg("leaf_diff equal keys, diff vals ~n"),
  Key = block_server:read_key(FileA, PtrA),
  leaf_diff(ValuesA, ValuesB, FileA, FileB, [{Key,ValA}|Keys]);
  
leaf_diff([{HashA, PtrA, ValA}|ValuesA], [{HashB, PtrB, ValB}|ValuesB], FileA, FileB, Keys) when HashA < HashB ->
  % error_logger:info_msg("leaf_diff complete diff ~p < ~p ~n", [HashA, HashB]),
  Key = block_server:read_key(FileA, PtrA),
  NewKeys = case lists:keytake(Key, 1, Keys) of
    {value, {Key, ValA}, Taken} -> Taken;
    {value, {Key, _}, _} -> Keys;
    false -> [{Key, ValA}|Keys]
  end,
  leaf_diff(ValuesA, [{HashB, PtrB, ValB}|ValuesB], FileA, FileB, NewKeys);
  
leaf_diff([{HashA, PtrA, ValA}|ValuesA], [{HashB, PtrB, ValB}|ValuesB], FileA, FileB, Keys) when HashA > HashB ->
  % error_logger:info_msg("leaf_diff complete diff ~p > ~p ~n", [HashA, HashB]),
  Key = block_server:read_key(FileB, PtrB),
  NewKeys = case lists:keytake(Key, 1, Keys) of
    {value, {Key, ValB}, Taken} -> Taken;
    {value, {Key, _}, _} -> Keys;
    false -> [{Key, ValB}|Keys]
  end,
  leaf_diff([{HashA, PtrA, ValA}|ValuesA], ValuesB, FileA, FileB, NewKeys).

leaves(#node{children=Children}, Tree = #dmerkle{file=File,block=BlockSize}, Key) ->
  lists:foldl(fun({_,Ptr}, Keys) ->
      Node = read(File, Ptr, BlockSize),
      leaves(Node, Tree, Keys)
    end, [], Children);
    
leaves(#leaf{values=Values}, Tree = #dmerkle{file=File,block=BlockSize}, Keys) ->
  lists:foldl(fun({_, Ptr, Val}, Keys) ->
      Key = block_server:read_key(File, Ptr),
      case lists:keytake(Key, 1, Keys) of
        {value, {Key, Val}, Taken} -> Taken;
        {value, {Key, _}, _} -> Keys;
        false -> [{Key, Val}|Keys]
      end
    end, Keys, Values).

find(KeyHash, Key, Node = #node{keys=Keys,children=Children}, Tree = #dmerkle{file=File,block=BlockSize}) ->
  {_FoundKey, {_,ChildPointer}} = find_child(KeyHash, Keys, Children),
  % error_logger:info_msg("finding keyhash ~p in ~p got ~p~n", [KeyHash, Keys, _FoundKey]),
  find(KeyHash, Key, read(File,ChildPointer,BlockSize), Tree);
  
find(KeyHash, Key, Leaf = #leaf{values=Values}, Tree = #dmerkle{file=File,block=BlockSize}) ->
  % error_logger:info_msg("looking for ~p in ~p~n", [KeyHash, Values]),
  case lists:keysearch(KeyHash, 1, Values) of
    {value, {KeyHash,_,ValHash}} -> ValHash;
    false -> not_found
  end.

update(KeyHash, Key, Value, Node = #node{children=Children,keys=Keys}, Tree = #dmerkle{d=D,file=File,block=BlockSize}) ->
  {FoundKey, {ChildHash,ChildPointer}} = find_child(KeyHash, Keys, Children),
  if
    ChildPointer == 0 -> error_logger:info_msg("reading child at ~p~n for node with M ~p keys ~p children ~p~n", [ChildPointer, m(Node), length(Keys), length(Children)]);
    true -> ok
  end,
  Child = read(File,ChildPointer,BlockSize),
  case m(Child) of
    M when M >= D -> update(KeyHash, Key, Value, split_child(Node, FoundKey, Child, Tree), Tree);
    _ -> 
      NewChildHash = hash(update(KeyHash, Key, Value, Child, Tree)),
      write(File, BlockSize, Node#node{m=length(Node#node.keys),children=lists:keyreplace(ChildPointer, 2, Children, {NewChildHash,ChildPointer})})
  end;
  
update(KeyHash, Key, Value, Leaf = #leaf{values=Values}, Tree = #dmerkle{d=D,file=File,block=BlockSize}) ->
  NewValHash = hash(Value),
  case lists:keysearch(KeyHash, 1, Values) of
    {value, {KeyHash,Pointer,ValHash}} ->
      case block_server:read_key(File, Pointer) of
        Key -> 
          % error_logger:info_msg("we found the key ~p~n", [Key]),
          write(File, BlockSize, Leaf#leaf{
              values=lists:keyreplace(KeyHash, 1, Values, {KeyHash,Pointer,NewValHash})
            });
        _ ->  %we still need to deal with collision here
          % error_logger:info_msg("hash found but no key found, inserting new ~n"),
          {ok, NewPointer} = block_server:write_key(File, eof, Key),
          write(File, BlockSize, Leaf#leaf{
              values=lists:keymerge(1, Values, [{KeyHash,NewPointer,NewValHash}])
            })
      end;
    false ->
      % error_logger:info_msg("no hash or key found, inserting new ~n"),
      {ok, NewPointer} = block_server:write_key(File, eof, Key),
      NewValues = lists:keymerge(1, Values, [{KeyHash,NewPointer,NewValHash}]),
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

split_child(_, empty, Child = #node{m=M,keys=Keys,children=Children}, #dmerkle{file=File,block=BlockSize}) ->
  {LeftKeys, [_ | RightKeys]} = lists:split((M div 2)-1, Keys),
  {LeftChildren, RightChildren} = lists:split(M div 2, Children),
  % error_logger:info_msg("splitchild(empty rightkeys ~p rightchildren ~p leftkeys ~p leftchildren ~p~n", [length(RightKeys), length(RightChildren), length(LeftKeys), length(LeftChildren)]),
  Left = write(File, BlockSize, #node{m=length(LeftKeys),keys=LeftKeys,children=LeftChildren}),
  Right = write(File, BlockSize, Child#node{m=length(RightKeys),keys=RightKeys,children=RightChildren}),
  write(File, BlockSize, #node{m=1,
    keys=[lists:last(LeftKeys)],
    children=[{hash(Left),offset(Left)},{hash(Right),offset(Right)}]});

split_child(Parent = #node{keys=Keys,children=Children}, ToReplace, Child = #leaf{values=Values,m=M}, #dmerkle{file=File,block=BlockSize}) ->
  % error_logger:info_msg("splitting leaf with offset~p parent with offset~p ~n", [Child#leaf.offset, Parent#node.offset]),
  {KeyHash, _, _} = lists:nth(M div 2, Values),
  {LeftValues, RightValues} = lists:partition(fun({Hash,_,_}) ->
      Hash =< KeyHash
    end, Values),
  % error_logger:info_msg("split_child(leaf left ~p right ~p orig ~p~n", [length(LeftValues), length(RightValues), length(Values)]),
  % error_logger:info_msg("lhas ~p rhas ~p orighas ~p~n", [lists:keymember(3784569674, 1, LeftValues), lists:keymember(3784569674, 1, RightValues), lists:keymember(3784569674, 1, Values)]),
  Left = write(File, BlockSize, #leaf{m=length(LeftValues),values=LeftValues}),
  Right = write(File, BlockSize, Child#leaf{m=length(RightValues),values=RightValues}),
  write(File, BlockSize, replace(Parent, ToReplace, Left, Right, last_key(Left)));
  
split_child(Parent = #node{keys=Keys,children=Children}, ToReplace, Child = #node{m=M,keys=ChildKeys,children=ChildChildren}, #dmerkle{file=File,block=BlockSize}) ->
  % error_logger:info_msg("splitting node ~p~n", [Parent]),
  % KeyHash = lists:nth(M div 2, Keys),
  {PreLeftKeys, RightKeys} = lists:split(M div 2, ChildKeys),
  {LeftChildren, RightChildren} = lists:split(M div 2, ChildChildren),
  [LeftKeyHash| ReversedLeftKeys] = lists:reverse(PreLeftKeys),
  LeftKeys = lists:reverse(ReversedLeftKeys),
  % error_logger:info_msg("split_child(node rightkeys ~p rightchildren ~p leftkeys ~p leftchildren ~p~n", [length(RightKeys), length(RightChildren), length(LeftKeys), length(LeftChildren)]),
  Left = write(File, BlockSize, #node{m=length(LeftKeys),keys=LeftKeys,children=LeftChildren}),
  Right = write(File, BlockSize, Child#node{m=length(RightKeys),keys=RightKeys,children=RightChildren}),
  write(File, BlockSize, replace(Parent, ToReplace, Left, Right, LeftKeyHash)).

replace(Parent = #node{keys=Keys,children=Children}, empty, Left, Right, KeyHash) ->
  Parent#node{
    m=1,
    keys=[KeyHash],
    children=[{hash(Left),offset(Left)},{hash(Right),offset(Right)}]
  };
  
replace(Parent = #node{keys=Keys,children=Children}, last, Left, Right, KeyHash) ->
  Parent#node{
    m=length(Keys)+1,
    keys=Keys ++ [KeyHash],
    children = lists:sublist(Children, length(Children)-1) ++ 
      [{hash(Left),offset(Left)}, {hash(Right),offset(Right)}]
  };

replace(Parent = #node{keys=Keys,children=Children}, ToReplace, Left, Right, KeyHash) ->
  N = lib_misc:position(ToReplace, Keys),
  % KeyHash = last_key(Left),
  % error_logger:info_msg("replace toreplace ~p n ~p keyhash ~p keys ~p~n children ~p~n left ~p~n right ~p~n", [ToReplace, N, KeyHash, Keys, Children, Left, Right]),
  KeyTail = if
    N-1 >= length(Keys) -> [];
    true -> lists:nthtail(N-1, Keys)
  end,
  ChildTail = if
    N >= length(Children) -> [];
    true -> lists:nthtail(N, Children)
  end,
  Parent#node{
    keys = lists:sublist(Keys, N-1) ++ [KeyHash] ++ KeyTail,
    children = lists:sublist(Children, N-1) ++ 
      [{hash(Left), offset(Left)}, {hash(Right), offset(Right)}] ++ 
      ChildTail
  }.

create_or_read_root(File, BlockSize) ->
  case block_server:read_block(File,4,8) of
    eof -> 
      % error_logger:info_msg("could not find offset ~n"),
      block_server:write_block(File,4,<<0:64>>), %placeholder
      Root = write(File, BlockSize, #leaf{}),
      update_root_pointer(File, Root),
      Root;
    {ok, Bin} -> 
      <<Offset:64>> = Bin,
      % error_logger:info_msg("read offset ~p ~p~n", [Bin, Offset]),
      read(File, Offset, BlockSize)
  end.

update_root_pointer(File, Root) ->
  Offset = offset(Root),
  % error_logger:info_msg("writing root offset ~p~n", [Offset]),
  {ok, 4} = block_server:write_block(File,4,<<Offset:64>>).

%node is denoted by a 0
deserialize(<<0:8, Binary/binary>>, Offset) ->
  D = d_from_blocksize(byte_size(Binary) + 1),
  KeyBinSize = D*4,
  ChildBinSize = (D+1)*12,
  <<M:32, KeyBin:KeyBinSize/binary, ChildBin:ChildBinSize/binary>> = Binary,
  if
    M > D -> error_logger:info_msg("M is larger than D M ~p D ~p offset~p~n", [M, D, Offset]);
    true -> ok
  end,
  Keys = unpack_keys(M, KeyBin),
  Children = unpack_children(M+1, ChildBin),
  #node{m=M,children=Children,keys=Keys,offset=Offset};
  
deserialize(<<1:8, Bin/binary>>, Offset) ->
  D = d_from_blocksize(byte_size(Bin) + 1),
  ValuesBinSize = D*16,
  <<M:32, ValuesBin:ValuesBinSize/binary, _/binary>> = Bin,
  Values = unpack_values(M, ValuesBin),
  #leaf{m=M,values=Values,offset=Offset}.
  
serialize(Node = #node{keys=Keys,children=Children,m=M}, BlockSize) ->
  D = d_from_blocksize(BlockSize),
  if
    M > D -> error_logger:info_msg("M is larger than D M ~p D ~p~n", [M, D]);
    length(Keys) == length(Children) -> error_logger:info_msg("There are as many children as keys for ~p~n", [Node]);
    true -> ok
  end,
  KeyBin = pack_keys(Keys, D),
  ChildBin = pack_children(Children, D+1),
  LeftOverBits = (BlockSize - byte_size(KeyBin) - byte_size(ChildBin) - 5)*8,
  OutBin = <<0:8, M:32, KeyBin/binary, ChildBin/binary, 0:LeftOverBits>>,
  if 
    byte_size(OutBin) /= BlockSize ->
      error_logger:info_msg("outbin is wrong size! keys: ~p children: ~p m: ~p outbin ~p~n", [length(Keys), length(Children), M, byte_size(OutBin)]);
    true -> ok
  end,
  OutBin;
  
serialize(#leaf{values=Values,m=M}, BlockSize) ->
  D = d_from_blocksize(BlockSize),
  if
    M > D -> error_logger:info_msg("M is larger than D M ~p D ~p~n", [M, D]);
    true -> ok
  end,
  ValuesBin = pack_values(Values),
  LeftOverBits = (BlockSize - byte_size(ValuesBin) - 5)*8,
  OutBin = <<1:8, M:32, ValuesBin/binary, 0:LeftOverBits>>,
  if 
    byte_size(OutBin) /= BlockSize ->
      error_logger:info_msg("outbin is wrong size! values: ~p m: ~p outbin ~p~n", [length(Values), M, byte_size(OutBin)]);
    true -> ok
  end,
  OutBin.
  
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
  if
    SkipSize+4 > byte_size(Bin) ->
      error_logger:info_msg("Whoops, ran out of unpack space M ~p N ~p Bin ~p~n", [M, N, byte_size(Bin)]);
    true -> noop
  end,
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
  if
    Offset == 0 -> error_logger:info_msg("writing node at 0: BlockSize ~p, M ~p byte_size(Bin): ~p~n", [BlockSize, m(Node), byte_size(Bin)]);
    true -> ok
  end,
  % error_logger:info_msg("writing out node ~p~n", [Node]),
  
  {ok, NewOffset} = block_server:write_block(File,Offset,Bin),
  if
    NewOffset =< 4 -> error_logger:info_msg("overwrote offset!~n");
    true -> ok
  end,
  % error_logger:info_msg("new offset ~p~n", [NewOffset]),
  offset(Node, NewOffset).
  
read(File, Offset, BlockSize) ->
  case block_server:read_block(File, Offset, BlockSize) of
    {ok, Bin} -> deserialize(Bin, Offset);
    eof -> error_logger:info_msg("hit an eof for offset ~p", [Offset]),
      undefined;
    {error, Reason} -> error_logger:info_msg("error ~p at offset", [Reason, Offset]),
      undefined
  end.
  
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

%%%
% hashes need to be based off of value hashes, not anything storage specific
hash(Node = #node{children=Children}) ->
  lists:sum(lists:map(fun({Hash, _Pointer}) -> Hash end, Children)) rem (2 bsl 31);
  
hash(Leaf = #leaf{values=Values}) ->
  lists:sum(lists:map(fun({_, _, Hash}) -> Hash end, Values)) rem (2 bsl 31);
  
hash(V) -> lib_misc:hash(V).