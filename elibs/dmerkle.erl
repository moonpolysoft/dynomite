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
-author('cliff@powerset.com').

-behavior(gen_server).

-record(dmerkle, {file, block, root, d, filename, freepointer=0, rootpointer=0, fp1=0, fp2=0, fp3=0, fp4=0, fp5=0}).

-record(node, {m=0, keys=[], children=[], offset=eof}).
-record(leaf, {m=0, values=[], offset=eof}).
-record(free, {offset,pointer=0}).

-include("common.hrl").

-define(VERSION, 1).
-define(HEADER_SIZE, 125).
-define(ROOT_POS, (1+4+8)).
-define(FREE_POS, (?ROOT_POS+8)).

%% API
-export([open/1, open/2, equals/2, get_tree/1, count/2, count_trace/2, update/3, delete/2, leaves/1, find/2, visualized_find/2, key_diff/2, close/1, scan_for_empty/1, swap_tree/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-ifdef(TEST).
-include("etest/dmerkle_test.erl").
-endif.

%%====================================================================
%% Public API
%%====================================================================

open(FileName) ->
  open(FileName, undefined).

open(FileName, BlockSize) -> 
  gen_server:start_link(?MODULE, {FileName,BlockSize}, []).

count_trace(Pid, Key) ->
  gen_server:call(Pid, {count_trace, Key}).

count(Pid, Level) ->
  gen_server:call(Pid, {count, Level}).

leaves(Pid) ->
  gen_server:call(Pid, leaves).

update(Key, Value, Pid) ->
  gen_server:call(Pid, {update, Key, Value}).

equals(A, B) ->
  gen_server:call(A, hash) == gen_server:call(B, hash).

find(Key, Tree) ->
  gen_server:call(Tree, {find, Key}).
  
visualized_find(Key, Tree) ->
  gen_server:call(Tree, {visualized_find, Key}).

delete(Key, Tree) ->
  gen_server:call(Tree, {delete, Key}).

key_diff(TreeA, TreeB) ->
  gen_server:call(TreeA, {key_diff, TreeB}).

close(Tree) ->
  gen_server:cast(Tree, close).

scan_for_empty(Tree) ->
  gen_server:call(Tree, scan_for_empty).

get_tree(Tree) ->
  gen_server:call(Tree, get_tree).

swap_tree(OldTree, NewTree) ->
  NewFilename = gen_server:call(NewTree, filename),
  BlockSize = gen_server:call(OldTree, blocksize),
  OldFilename = gen_server:call(OldTree, filename),
  close(OldTree),
  close(NewTree),
  file:copy(block_server:index_name(NewFilename), block_server:index_name(OldFilename)),
  file:copy(block_server:key_name(NewFilename), block_server:key_name(OldFilename)),
  file:delete(block_server:index_name(NewFilename)),
  file:delete(block_server:key_name(NewFilename)),
  open(OldFilename, BlockSize).

  %%====================================================================
%% gen_server callbacks
%%====================================================================

%%--------------------------------------------------------------------
%% @spec init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% @doc Initiates the server
%% @end 
%%--------------------------------------------------------------------
init({FileName, BlockSize}) ->
  filelib:ensure_dir(FileName),
  {ok, File} = block_server:start_link(FileName, BlockSize),
  Tree = case block_server:read_block(File, 0, ?HEADER_SIZE) of
    {ok, BinHeader} -> deserialize_header(BinHeader, #dmerkle{file=File,filename=FileName});
    eof ->
      D = d_from_blocksize(BlockSize),
      ModBlockSize = blocksize_from_d(D),
      T = #dmerkle{file=File,d=D,block=ModBlockSize,freepointer=0,rootpointer=0,filename=FileName},
      block_server:write_block(File, 0, serialize_header(T)),
      T
  end,
  case Tree of
    {error, Msg} -> {stop, Msg};
    #dmerkle{} -> {ok, create_or_read_root(Tree)}
  end.

%%--------------------------------------------------------------------
%% @spec 
%% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% @doc Handling call messages
%% @end 
%%--------------------------------------------------------------------
handle_call({count_trace, Key}, _From, Tree = #dmerkle{root=Root}) ->
  Reply = count_trace(Tree, Root, hash(Key)),
  {reply, Reply, Tree};
  
handle_call({count, Level}, _From, Tree = #dmerkle{root=Root}) ->
  Reply = count(Tree, Root, Level),
  {reply, Reply, Tree};
  
handle_call(leaves, _From, Tree = #dmerkle{root=Root}) ->
  Reply = leaves(Root, Tree, []),
  {reply, Reply, Tree};
  
handle_call({update, Key, Value}, _From, Tree = #dmerkle{file=File,block=BlockSize,d=D,root=Root}) ->
  % error_logger:info_msg("inserting ~p~n", [Key]),
  M = m(Root),
  NewTree = if
    M >= D-1 -> %allocate new root, move old root and split
      {Root2, Tree2} = split_child(#node{}, empty, Root, Tree),
      Tree3 = update_root(Tree2, Root2),
      % error_logger:info_msg("found: ~p~n", [visualized_find("key60", Tree#dmerkle{root=FinalRoot})]),
      {Root3, Tree4} = update(hash(Key), Key, Value, Root2, Tree3),
      Tree3#dmerkle{root=Root3};
    true -> 
      {Root2, Tree2} = update(hash(Key), Key, Value, Root, Tree),
      Tree2#dmerkle{root=Root2}
  end,
  {reply, self(), NewTree};
  
handle_call({find, Key}, _From, Tree = #dmerkle{root=Root}) ->
  Reply = find(hash(Key), Key, Root, Tree),
  {reply, Reply, Tree};
  
handle_call(hash, _From, Tree = #dmerkle{root=Root}) ->
  {reply, hash(Root), Tree};
  
handle_call({visualized_find, Key}, _From, Tree = #dmerkle{root=Root}) ->
  Reply = visualized_find(hash(Key), Key, Root, Tree, []),
  {reply, Reply, Tree};
  
handle_call({delete, Key}, _From, Tree = #dmerkle{root=Root}) ->
  {RetNode, NewTree} = delete(hash(Key), Key, root, Root, Tree),
  case ref_equals(RetNode, Root) of
    true -> {reply, self(), NewTree#dmerkle{root=RetNode}};
    false -> {reply, self(), NewTree}
  end;
  
handle_call(blocksize, _From, Tree = #dmerkle{block=BlockSize}) ->
  {reply, BlockSize, Tree};
  
handle_call(get_tree, _From, Tree) ->
  {reply, Tree, Tree};
  
handle_call({key_diff, PidB}, _From, TreeA = #dmerkle{root=RootA}) ->
  Reply = if
    self() == PidB -> {error, "Cannot do a diff on the same merkle tree."};
    true ->
      TreeB = gen_server:call(PidB, get_tree),
      RootB = TreeB#dmerkle.root,
      {KeysA, KeysB} = key_diff(RootA, RootB, TreeA, TreeB, [], []),
      lists:usort(diff_merge(TreeA, TreeB, lists:keysort(1, KeysA), lists:keysort(1, KeysB), []))
  end,
  {reply, Reply, TreeA};
  
handle_call(filename, _From, Tree = #dmerkle{filename=Filename}) ->
  {reply, Filename, Tree};
  
handle_call(scan_for_empty, _From, Tree = #dmerkle{root=Root}) ->
  Reply = scan_for_empty(Tree, Root),
  {reply, Reply, Tree};
  
handle_call(Anything, _From, Tree) ->
  error_logger:info_msg("got unhandled call ~p~n", [Anything]),
  {reply, ok, Tree}.

%%--------------------------------------------------------------------
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% @doc Handling cast messages
%% @end 
%%--------------------------------------------------------------------
handle_cast(close, Tree) ->
    {stop, shutdown, Tree}.

%%--------------------------------------------------------------------
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% @doc Handling all non call/cast messages
%% @end 
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @spec terminate(Reason, State) -> void()
%% @doc This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%% @end 
%%--------------------------------------------------------------------
terminate(_Reason, #dmerkle{file=File}) ->
    block_server:stop(File).

%%--------------------------------------------------------------------
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @doc Convert process state when code is changed
%% @end 
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%====================================================================
%% Internal functions
%%====================================================================
delete(KeyHash, Key, Parent, Node = #node{children=Children,keys=Keys}, Tree = #dmerkle{d=D,file=File,block=BlockSize}) ->
  error_logger:info_msg("delete key ~p keyhash ~p from node ~p~n", [Key, KeyHash, Node]),
  {WhatItDo, NewTree, DeleteNode} = case find_child_adj(KeyHash, Keys, Children) of
    {FoundKey, {{LeftHash, LeftPointer}, {RightHash, RightPointer}}} ->
      LeftNode = read(File,LeftPointer,BlockSize),
      RightNode = read(File, RightPointer, BlockSize),
      ?infoFmt("delete_merge foundkey ~p~nLeftPointer ~p~nrightpointer ~p~nD ~p~nparent ~p~nnode ~p~nleftnode ~p~nrightnode ~p~n", [FoundKey, LeftPointer, RightPointer, D, Parent, Node, LeftNode, RightNode]),
      delete_merge(FoundKey, Parent, Node, LeftNode, RightNode, Tree);
    {FoundKey, {{LeftHash, LeftPointer}, undefined}} ->
      {nothing, Tree, read(File, LeftPointer, BlockSize)}
  end,
  ?infoMsg("recursing into delete~n"),
  {ReturnNode, FinalTree} = delete(KeyHash, Key, Node, DeleteNode, NewTree),
  ?infoFmt("reduced from delete ~p~n", [{ReturnNode, FinalTree}]),
  Eqls = ref_equals(ReturnNode, Node),
  case WhatItDo of
    wamp_wamp -> {ReturnNode, FinalTree};
    _ when Eqls -> {ReturnNode, FinalTree};
    _ -> update_hash(hash(ReturnNode), offset(ReturnNode), Node, FinalTree)
  end;

delete(KeyHash, Key, Parent, Leaf = #leaf{values=Values}, Tree = #dmerkle{root=Root, file=File,block=BlockSize}) ->
  error_logger:info_msg("delete key ~p keyhash ~p from ~p~n", [Key, KeyHash, Leaf]),
  case lists:keytake(KeyHash, 1, Values) of
    {value, {KeyHash,Pointer,ValHash}, NewValues} ->
      NewLeaf = Leaf#leaf{values=NewValues,m=length(NewValues)},
      ?infoFmt("new leaf ~p~n", [NewLeaf]),
      {NewLeaf2, Tree2} = write(NewLeaf, free_key(Key, Pointer, Tree)),
      if
        Leaf == Root -> 
          error_logger:info_msg("replacing leaf ~p~n", [NewLeaf2]),
          {NewLeaf, Tree2#dmerkle{root=NewLeaf2}};
        true -> 
          {NewLeaf, Tree2}
      end;
    false -> 
      ?infoFmt("couldnt find ~p in ~p~n", [KeyHash, Leaf]),
      {Leaf, Tree}
  end.

update_hash(Hash, Pointer, Node = #node{children=Children}, Tree = #dmerkle{root=Root,file=File,block=BlockSize}) ->
  ?infoFmt("updating hash,ptr ~p for ~p~n", [{Hash,Pointer}, Node]),
  NewNode = Node#node{children=lists:keyreplace(Pointer, 2, Children, {Hash,Pointer})},
  ?infoFmt("updated node ~p~n", [NewNode]),
  write(NewNode, Tree).

% delete_merge(FoundKey,
%              Parent = #node{keys=PKeys,children=PChildren,m=M})

% we have to replace the parent in this case with the merged leaf
%%merging leaves
delete_merge(FoundKey,
             root,
             Root = #node{keys=PKeys,children=PChildren,m=PM},
             LeftLeaf = #leaf{values=LeftValues,m=LeftM},
             RightLeaf = #leaf{values=RightValues,m=RightM},
             Tree = #dmerkle{block=BlockSize,file=File,d=D}) when (LeftM+RightM) =< D, 
                                                                  PM == 1 ->
  ?infoMsg("Replacing root merging leaves~n"),
  Tree2 = delete_cell(RightLeaf#leaf.offset, delete_cell(Root#node.offset, Tree)),
  {NewLeaf, Tree3} = write(LeftLeaf#leaf{m=LeftM+RightM,values=LeftValues++RightValues}, Tree2),
  {wamp_wamp, write_header(Tree3#dmerkle{root=NewLeaf}), NewLeaf};
  
delete_merge(FoundKey,
             SuperParent = #node{children=SPChildren},
             Parent = #node{keys=PKeys,children=PChildren,m=PM},
             LeftLeaf = #leaf{values=LeftValues,m=LeftM},
             RightLeaf = #leaf{values=RightValues,m=RightM},
             Tree = #dmerkle{block=BlockSize,file=File,d=D,root=Root}) when (LeftM+RightM) =< D, 
                                                                            length(PKeys) == 1 ->
  ?infoMsg("Replacing node merging leaves~n"),
  Tree2 = delete_cell(RightLeaf#leaf.offset, delete_cell(Parent#node.offset, Tree)),
  {NewLeaf, Tree3} = write(LeftLeaf#leaf{m=LeftM+RightM,values=LeftValues++RightValues}, Tree2),
  {NP, Tree4} = write(SuperParent#node{children=lists:keyreplace(offset(Parent), 2, PChildren, {hash(NewLeaf),offset(NewLeaf)})}, Tree3),
  ?infoFmt("replaced ~p in super parent ~p~n", [{hash(Parent),offset(Parent)}, NP]),
  {wamp_wamp, Tree4, NewLeaf};
  
delete_merge(FoundKey,
             SuperParent,
             Parent = #node{keys=PKeys,children=PChildren,m=PM}, 
             LeftLeaf = #leaf{values=LeftValues,m=LeftM}, 
             RightLeaf = #leaf{values=RightValues,m=RightM}, 
             Tree = #dmerkle{block=BlockSize,file=File,d=D}) when (LeftM+RightM) =< D ->
  %we can merge within reqs gogogo
  ?infoMsg("Merging leaves~n"),
  N = if
    FoundKey == last -> length(PKeys);
    true -> lib_misc:position(FoundKey, PKeys)
  end,
  ?infoFmt("FoundKey ~p~nPKeys ~p~nPChildren ~p~nN ~p~nLeftM ~p~nRightM ~p~nD ~p~n", [FoundKey, PKeys, PChildren, N, LeftM, RightM, D]),
  {NP, Tree2} = write(remove_nth(Parent, N), Tree),
  {_, Tree3} = write(LeftLeaf#leaf{m=LeftM+RightM,values=LeftValues++RightValues}, Tree2),
  ?infoFmt("new parent: ~p~n", [NP]),
  { merge, delete_cell(RightLeaf#leaf.offset, Tree3), NP};
  
%merging nodes
delete_merge(FoundKey,
             root,
             Root = #node{keys=PKeys,children=PChildren,m=PM},
             LeftNode = #node{m=LeftM},
             RightNode = #node{m=RightM},
             Tree = #dmerkle{block=BlockSize,file=File,d=D}) when (LeftM+RightM) < D,
                                                                  length(PKeys) == 1 ->
  ?infoMsg("replacing root merging nodes~n"),
  {NC, Tree2} = write(merge_nodes(FoundKey, PKeys, LeftNode, RightNode), Tree),
  Tree3 = delete_cell(offset(RightNode), delete_cell(offset(Root), Tree2)),
  {wamp_wamp, write_header(Tree3#dmerkle{root=NC}), NC};

  
delete_merge(FoundKey,
             SuperParent = #node{children=SPChildren},
             Parent = #node{keys=PKeys,children=PChildren,m=PM},
             LeftNode = #node{m=LeftM,keys=LeftKeys,children=LeftChildren},
             RightNode = #node{m=RightM,keys=RightKeys,children=RightChildren},
             Tree = #dmerkle{block=BlockSize,file=File,d=D,root=Root}) when (LeftM+RightM) < D, 
                                                                            length(PKeys) == 1 ->
  ?infoMsg("Replacing node merging nodes~n"),
  Tree2 = delete_cell(offset(RightNode), delete_cell(offset(Parent), Tree)),
  ParentPointer = offset(Parent),
  ParentHash = hash(Parent),
  NN = merge_nodes(FoundKey, PKeys, LeftNode, RightNode),
  {NP, Tree3} = write(SuperParent#node{children=lists:keyreplace(offset(Parent), 2, PChildren, {hash(NN),offset(NN)})}, Tree2),
  ?infoFmt("NN: ~p~n", [NN]),
  {NewNode, Tree4} = write(NN, Tree3),
  {wamp_wamp, Tree4, NewNode};

delete_merge(FoundKey,
             SuperParent,
             Parent = #node{keys=PKeys,children=PChildren,m=PM},
             LeftNode = #node{keys=LeftKeys,children=LeftChildren,m=LM},
             RightNode = #node{keys=RightKeys,children=RightChildren,m=RM},
             Tree = #dmerkle{block=BlockSize,file=File,d=D})  when (LM+RM) < D ->
  ?infoMsg("merging nodes~n"),
  N = if
    FoundKey == last -> length(PKeys) -1;
    true -> lib_misc:position(FoundKey, PKeys)
  end,
  ?infoFmt("FoundKey ~p~nPKeys ~p~nPChildren ~p~nN ~p~nLeftM ~p~nRightM ~p~nD ~p~n", [FoundKey, PKeys, PChildren, N, LM, RM, D]),
  {NP, Tree2} = write(remove_nth(Parent, N), Tree),
  {NC, Tree3} = write(merge_nodes(FoundKey, PKeys, LeftNode, RightNode), Tree2),
  ?infoFmt("new child: ~p~n", [NC]),
  ?infoFmt("new parent: ~p~n", [NP]),
  {merge, delete_cell(RightNode#node.offset, Tree3), NP};
  
delete_merge(last, _, _, _, Right, Tree) ->
  ?infoMsg("not merging~n"),
  {nothing, Tree, Right};
  
delete_merge(_, _, _, Left, _, Tree) ->
  %merged leaf is too large, do not merge
  ?infoMsg("not merging~n"),
  {nothing, Tree, Left}.
    
merge_nodes(FoundKey, PKeys, LeftNode = #node{m=LeftM,keys=LeftKeys,children=LeftChildren}, RightNode = #node{m=RightM,keys=RightKeys,children=RightChildren}) ->
  SplitKey = if
    FoundKey == last -> lists:last(PKeys);
    true -> FoundKey
  end,
  LeftNode#node{
    m=LeftM+RightM+1,
    keys=LeftKeys++[SplitKey]++RightKeys,
    children=LeftChildren++RightChildren
  }.
  
remove_nth(Node = #node{m=M,keys=Keys,children=Children}, N) ->
  Node#node{
    m=M-1,
    keys = lib_misc:nthdelete(N, Keys),
    children = lib_misc:nthdelete(N+1, Children)}.
  
%needs fixin
delete_cell(Offset, Tree = #dmerkle{file=File,block=BlockSize,freepointer=Pointer}) ->
  {_, Tree2} = write(#free{offset=Offset,pointer=Pointer}, Tree),
  write_header(Tree2#dmerkle{freepointer=Offset}).

count_trace(#dmerkle{file=File,block=BlockSize}, #leaf{values=Values}, Hash) ->
  length(lists:filter(fun({H, _, _}) -> 
      H == Hash
    end, Values));
    
count_trace(Tree = #dmerkle{file=File,block=BlockSize}, #node{children=Children}, Hash) ->
  Counts = lists:reverse(lists:foldl(fun({_, Ptr}, Acc) ->
      Child = read(File, Ptr, BlockSize),
      [count_trace(Tree, Child, Hash) | Acc]
    end, [], Children)).

count(#dmerkle{}, _, 0) ->
  1;
  
count(Tree = #dmerkle{file=File,block=BlockSize}, Node = #node{children=Children}, Level) ->
  lists:foldl(fun({_,Ptr}, Acc) ->
      Child = read(File, Ptr, BlockSize),
      Acc + count(Tree, Child, Level-1)
    end, 0, Children);
    
count(#dmerkle{}, #leaf{values=Values}, _) ->
  length(Values).

diff_merge(TreeA, TreeB, [], [], Ret) ->
  lists:sort(Ret);

diff_merge(TreeA = #dmerkle{file=File}, TreeB, [{Hash,Ptr,_}|KeysA], [], Ret) ->
  Key = block_server:read_key(File, Ptr),
  if
    Key == eof -> error_logger:info_msg("Key is eof: ([{~p, ~p}|KeysA], [], Ret)~n", [Hash, Ptr]);
    true -> ok
  end,
  diff_merge(TreeA, TreeB, KeysA, [], [Key|Ret]);

diff_merge(TreeA, TreeB = #dmerkle{file=File}, [], [{Hash,Ptr,_}|KeysB], Ret) ->
  Key = block_server:read_key(File, Ptr),
  if
    Key == eof -> error_logger:info_msg("Key is eof: ([], [{~p, ~p}|KeysB], Ret)~n", [Hash, Ptr]);
    true -> ok
  end,
  diff_merge(TreeA, TreeB, [], KeysB, [Key|Ret]);

diff_merge(TreeA = #dmerkle{file=FileA}, TreeB = #dmerkle{file=FileB}, [{Hash,PtrA,Val}|KeysA], [{Hash,PtrB,Val}|KeysB], Ret) ->
  diff_merge(TreeA, TreeB, KeysA, KeysB, Ret);

diff_merge(TreeA = #dmerkle{file=FileA}, TreeB = #dmerkle{file=FileB}, [{Hash,PtrA,_}|KeysA], [{Hash,PtrB,_}|KeysB], Ret) ->
  Key = block_server:read_key(FileA, PtrA),
  if
    Key == eof -> error_logger:info_msg("Key is eof: ([{~p, ~p}|KeysA], [{~p, ~p}|KeysB], Ret)~n", [Hash, PtrA, Hash, PtrB]);
    true -> ok
  end,
  diff_merge(TreeA, TreeB, KeysA, KeysB, [Key|Ret]);

diff_merge(TreeA = #dmerkle{file=FileA}, TreeB = #dmerkle{file=FileB}, [{HashA,PtrA,_}|KeysA], [{HashB,PtrB,ValB}|KeysB], Ret) when HashA < HashB ->
  KeyA = block_server:read_key(FileA, PtrA),
  diff_merge(TreeA, TreeB, KeysA, [{HashB,PtrB,ValB}|KeysB], [KeyA|Ret]);

diff_merge(TreeA = #dmerkle{file=FileA}, TreeB = #dmerkle{file=FileB}, [{HashA,PtrA,ValA}|KeysA], [{HashB,PtrB,_}|KeysB], Ret) when HashA > HashB ->
  KeyB = block_server:read_key(FileB, PtrB),
  diff_merge(TreeA, TreeB, [{HashA,PtrA,ValA}|KeysA], KeysB, [KeyB|Ret]).

foldl(Fun, Acc, Tree = #dmerkle{root=Root}) ->
  foldl(Fun, Acc, Root, Tree).
  
foldl(Fun, Acc, Node = #node{children=Children}, Tree = #dmerkle{file=File,block=BlockSize}) ->
  Acc2 = Fun(Node, Acc),
  lists:foldl(fun({_,Pointer}, A) ->
      foldl(Fun, A, read(File, Pointer, BlockSize), Tree)
    end, Acc2, Children);
  
foldl(Fun, Acc, Leaf = #leaf{}, Tree = #dmerkle{}) ->
  Fun(Leaf, Acc).
  
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
  
scan_for_nulls(Node = #node{children=Children}, Tree = #dmerkle{file=File,block=Block}) ->
  lists:foreach(fun
    ({_, 0}) ->
      ?infoFmt("has a zero: ~p~n",[Node]);
    ({_, ChldPtr}) ->
      % timer:sleep(1),
      scan_for_nulls(read(File,ChldPtr,Block), Tree)
    end, Children);
    
scan_for_nulls(#leaf{}, _) ->
  ok;
  
scan_for_nulls(_, _) ->
  ok.

key_diff(_LeafA = #leaf{values=ValuesA}, _LeafB = #leaf{values=ValuesB}, 
    #dmerkle{file=FileA}, #dmerkle{file=FileB}, KeysA, KeysB) ->
  leaf_diff(ValuesA, ValuesB, FileA, FileB, KeysA, KeysB);

key_diff(#node{children=ChildrenA}, #node{children=ChildrenB},
    TreeA = #dmerkle{file=FileA}, TreeB = #dmerkle{file=FileB}, KeysA, KeysB) ->
  % error_logger:info_msg("node differences ~n"),
  node_diff(ChildrenA, ChildrenB, TreeA, TreeB, KeysA, KeysB);
  
key_diff(Leaf = #leaf{}, Node = #node{children=Children}, TreeA, TreeB=#dmerkle{file=File,block=BlockSize}, KeysA, KeysB) ->
  % error_logger:info_msg("leaf node differences ~n"),
  lists:foldl(fun({_, Ptr}, {AccA, AccB}) ->
      Child = read(File, Ptr, BlockSize),
      key_diff(Leaf, Child, TreeA, TreeB, AccA, AccB)
    end, {KeysA, KeysB}, Children);
  
key_diff(Node = #node{children=Children}, Leaf = #leaf{}, TreeA=#dmerkle{file=File,block=BlockSize}, TreeB, KeysA, KeysB) ->
  % error_logger:info_msg("node leaf differences  ~n"),
  lists:foldl(fun({_, Ptr}, {AccA, AccB}) ->
      Child = read(File, Ptr, BlockSize),
      key_diff(Child, Leaf, TreeA, TreeB, AccA, AccB)
    end, {KeysA, KeysB}, Children).

node_diff([], [], TreeA, TreeB, KeysA, KeysB) -> {KeysA, KeysB};

node_diff([], ChildrenB, TreeA, TreeB = #dmerkle{file=File,block=BlockSize}, KeysA, KeysB) ->
  % error_logger:info_msg("node_diff empty children ~n"),
  {KeysA, lists:foldl(fun({_, Ptr}, Acc) ->
      Child = read(File, Ptr, BlockSize),
      hash_leaves(Child, TreeB, Acc)
    end, KeysB, ChildrenB)};
    
node_diff(ChildrenA, [], TreeA = #dmerkle{file=File,block=BlockSize}, TreeB, KeysA, KeysB) ->
  % error_logger:info_msg("node_diff children empty ~n"),
  {lists:foldl(fun({_, Ptr}, Acc) ->
      Child = read(File, Ptr, BlockSize),
      hash_leaves(Child, TreeA, Acc)
    end, KeysA, ChildrenA), KeysB};
    
node_diff([{Hash,PtrA}|ChildrenA], [{Hash,PtrB}|ChildrenB], TreeA, TreeB, KeysA, KeysB) ->
  % error_logger:info_msg("equal nodes ~n"),
  node_diff(ChildrenA, ChildrenB, TreeA, TreeB, KeysA, KeysB);
  
node_diff([{HashA,PtrA}|ChildrenA], [{HashB,PtrB}|ChildrenB], 
    TreeA=#dmerkle{file=FileA, block=BlockSizeA}, 
    TreeB=#dmerkle{file=FileB,block=BlockSizeB}, KeysA, KeysB) ->
  % error_logger:info_msg("nodes are different ~n"),
  ChildA = read(FileA, PtrA, BlockSizeA),
  ChildB = read(FileB, PtrB, BlockSizeB),
  {KeysA1, KeysB1} = key_diff(ChildA, ChildB, TreeA, TreeB, KeysA, KeysB),
  node_diff(ChildrenA, ChildrenB, TreeA, TreeB, KeysA1, KeysB1).

leaf_diff([], [], _, _, KeysA, KeysB) -> {KeysA, KeysB};

leaf_diff([], [{KeyHash, Ptr, Val}|ValuesB], FileA, FileB, KeysA, KeysB) ->
  % error_logger:info_msg("leaf_diff empty values ~n"),
  % NewKeys = case lists:keytake(KeyHash, 1, KeysB) of
  %   {value, {KeyHash, _, Val}, Taken} -> Taken;
  %   {value, {KeyHash, _, _}, _} -> KeysB;
  %   false -> [{KeyHash, Ptr, Val}|KeysB]
  % end,
  leaf_diff([], ValuesB, FileA, FileB, KeysA, [{KeyHash, Ptr, Val}|KeysB]);
  
leaf_diff([{KeyHash,Ptr,Val}|ValuesA], [], FileA, FileB, KeysA, KeysB) ->
  % error_logger:info_msg("leaf_diff values empty ~n"),
  % NewKeys = case lists:keytake(KeyHash, 1, KeysA) of
  %   {value, {KeyHash, _, Val}, Taken} -> Taken;
  %   {value, {KeyHash, _, _}, _} -> KeysA;
  %   false -> [{KeyHash, Ptr,Val}|KeysA]
  % end,
  leaf_diff(ValuesA, [], FileA, FileB, [{KeyHash, Ptr,Val}|KeysA], KeysB);
  
leaf_diff([{Hash, _, Val}|ValuesA], [{Hash, _, Val}|ValuesB], FileA, FileB, KeysA, KeysB) ->
  % error_logger:info_msg("leaf_diff equals ~n"),
  leaf_diff(ValuesA, ValuesB, FileA, FileB, KeysA, KeysB);
  
leaf_diff([{Hash, PtrA, ValA}|ValuesA], [{Hash, PtrB, ValB}|ValuesB], FileA, FileB, KeysA, KeysB) ->
  % error_logger:info_msg("leaf_diff equal keys, diff vals ~n"),
  leaf_diff(ValuesA, ValuesB, FileA, FileB, [{Hash,PtrA,ValA}|KeysA], KeysB);
  
leaf_diff([{HashA, PtrA, ValA}|ValuesA], [{HashB, PtrB, ValB}|ValuesB], FileA, FileB, KeysA, KeysB) when HashA < HashB ->
  % error_logger:info_msg("leaf_diff complete diff ~p < ~p ~n", [HashA, HashB]),
  % NewKeys = case lists:keytake(HashA, 1, KeysA) of
  %   {value, {HashA, _, ValA}, Taken} -> Taken;
  %   {value, {HashA, _, _}, _} -> KeysA;
  %   false -> [{HashA, PtrA, ValA}|KeysA]
  % end,
  leaf_diff(ValuesA, [{HashB, PtrB, ValB}|ValuesB], FileA, FileB, [{HashA, PtrA, ValA}|KeysA], KeysB);
  
leaf_diff([{HashA, PtrA, ValA}|ValuesA], [{HashB, PtrB, ValB}|ValuesB], FileA, FileB, KeysA, KeysB) when HashA > HashB ->
  % error_logger:info_msg("leaf_diff complete diff ~p > ~p ~n", [HashA, HashB]),
  % NewKeys = case lists:keytake(HashB, 1, KeysB) of
  %   {value, {HashB, _, ValB}, Taken} -> Taken;
  %   {value, {HashB, _, _}, _} -> KeysB;
  %   false -> [{HashB, PtrB, ValB}|KeysB]
  % end,
  leaf_diff([{HashA, PtrA, ValA}|ValuesA], ValuesB, FileA, FileB, KeysA, [{HashB, PtrB, ValB}|KeysB]).

hash_leaves(#node{children=Children}, Tree = #dmerkle{file=File,block=BlockSize}, Keys) ->
  lists:foldl(fun({_,Ptr}, Acc) ->
      Node = read(File, Ptr, BlockSize),
      hash_leaves(Node, Tree, Acc)
    end, Keys, Children);

hash_leaves(#leaf{values=Values}, Tree = #dmerkle{file=File,block=BlockSize}, Keys) ->
  lists:foldl(fun({KeyHash, Ptr, ValHash}, Acc) ->
      % case lists:keytake(KeyHash, 1, Acc) of
      %   {value, {KeyHash, _, ValHash}, Left} -> Left;
      %   {value, {KeyHash, _, _}, _} -> Acc;
      %   false -> [{KeyHash, Ptr, ValHash}|Acc]
      % end
      [{KeyHash, Ptr, ValHash}|Acc]
    end, Keys, Values).

leaves(#node{children=Children}, Tree = #dmerkle{file=File,block=BlockSize}, Keys) ->
  lists:foldl(fun({_,Ptr}, Acc) ->
      Node = read(File, Ptr, BlockSize),
      leaves(Node, Tree, Acc)
    end, Keys, Children);
    
leaves(#leaf{values=Values}, Tree = #dmerkle{file=File,block=BlockSize}, Keys) ->
  lists:foldl(fun({KeyHash, Ptr, ValHash}, Acc) ->
      Key = block_server:read_key(File, Ptr),
      % case lists:keytake(Key, 1, Acc) of
      %   {value, {Key, ValHash}, Left} -> Left;
      %   {value, {Key, _}, _} -> Acc;
      %   false -> [{Key, ValHash}|Acc]
      % end
      [{Key, ValHash}|Acc]
    end, Keys, Values).

visualized_find(KeyHash, Key, Node = #node{keys=Keys,children=Children}, Tree = #dmerkle{file=File,block=BlockSize}, Trail) ->
  {FoundKey, {_,ChildPointer}} = find_child(KeyHash, Keys, Children),
  {Before, After} = lists:partition(fun(E) -> E =< FoundKey end, Keys),
  % error_logger:info_msg("finding keyhash ~p in ~p got ~p~n", [KeyHash, Keys, _FoundKey]),
  visualized_find(KeyHash, Key, read(File,ChildPointer,BlockSize), Tree, [{FoundKey, offset(Node), length(Before)} | Trail]);

visualized_find(KeyHash, Key, Leaf = #leaf{values=Values}, Tree = #dmerkle{file=File,block=BlockSize}, Trail) ->
  % error_logger:info_msg("looking for ~p in ~p~n", [KeyHash, Values]),
  case lists:keysearch(KeyHash, 1, Values) of
    {value, {KeyHash,_,ValHash}} -> 
      error_logger:info_msg("find trail for ~p(~p): ~p~n", [Key, KeyHash, lists:reverse([{KeyHash, offset(Leaf)} | Trail])]),
      ValHash;
    false -> 
      error_logger:info_msg("find trail for ~p(~p): ~p~n", [Key, KeyHash, lists:reverse(Trail)]),
      not_found
  end.

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
  % error_logger:info_msg("update(~p, ~p, Value, #node ~p, Tree)~n", [KeyHash, Key, offset(Node)]),
  {FoundKey, {ChildHash,ChildPointer}} = find_child(KeyHash, Keys, Children),
  if
    ChildPointer == 0 -> error_logger:info_msg("reading child at ~p~n for node with M ~p keys ~p children ~p~n", [ChildPointer, m(Node), length(Keys), length(Children)]);
    true -> ok
  end,
  Child = read(File,ChildPointer,BlockSize),
  case m(Child) of
    M when M >= D -> 
      {Node2, Tree2} = split_child(Node, FoundKey, Child, Tree),
      update(KeyHash, Key, Value, Node2, Tree2);
    _ -> 
      {Child2, Tree2} = update(KeyHash, Key, Value, Child, Tree),
      write(Node#node{m=length(Node#node.keys),children=lists:keyreplace(ChildPointer, 2, Children, {hash(Child2),offset(Child2)})}, Tree2)
  end;
  
update(KeyHash, Key, Value, Leaf = #leaf{values=Values}, Tree = #dmerkle{d=D,file=File,block=BlockSize}) ->
  % error_logger:info_msg("update(~p, ~p, Value, #leaf ~p, Tree)~n", [KeyHash, Key, offset(Leaf)]),
  NewValHash = hash(Value),
  case lists:keysearch(KeyHash, 1, Values) of
    {value, {KeyHash,Pointer,ValHash}} ->
      case block_server:read_key(File, Pointer) of
        Key ->
          write(Leaf#leaf{values=lists:keyreplace(KeyHash, 1, Values, {KeyHash,Pointer,NewValHash})}, Tree);
        _ ->  %we still need to deal with collision here
          % error_logger:info_msg("hash found but no key found, inserting new ~n"),
          {NewPointer, Tree2} = allocate_key(Key, Tree),
          write(Leaf#leaf{values=lists:keymerge(1, Values, [{KeyHash,NewPointer,NewValHash}])}, Tree2)
      end;
    false ->
      % error_logger:info_msg("no hash or key found, inserting new ~n"),
      {ok, NewPointer} = block_server:write_key(File, eof, Key),
      NewValues = lists:keymerge(1, Values, [{KeyHash,NewPointer,NewValHash}]),
      write(Leaf#leaf{m=length(NewValues),values=NewValues}, Tree)
  end.
  
allocate_key(Key, Tree = #dmerkle{file=File}) ->
  {ok, NewPointer} = block_server:write_key(File, eof, Key).
  
free_key(Key, Pointer, Tree) ->
  Tree.
  
free_key_list(Size, #dmerkle{fp1=F}) when Size < 10 -> F;
free_key_list(Size, #dmerkle{fp2=F}) when Size < 100 -> F;
free_key_list(Size, #dmerkle{fp3=F}) when Size < 1000 -> F;
free_key_list(Size, #dmerkle{fp4=F}) when Size < 10000 -> F;
free_key_list(_, #dmerkle{fp5=F}) -> F.
  
set_pointer(Size, Pointer, Tree) when Size < 10 -> Tree#dmerkle{fp1=Pointer};
set_pointer(Size, Pointer, Tree) when Size < 100 -> Tree#dmerkle{fp2=Pointer};
set_pointer(Size, Pointer, Tree) when Size < 1000 -> Tree#dmerkle{fp3=Pointer};
set_pointer(Size, Pointer, Tree) when Size < 10000 -> Tree#dmerkle{fp4=Pointer};
set_pointer(Size, Pointer, Tree) -> Tree#dmerkle{fp5=Pointer}.
  
find_child(_, [], [Child]) ->
  {last, Child};
  
find_child(KeyHash, [Key|Keys], [Child|Children]) ->
  if
    KeyHash =< Key -> {Key, Child};
    true -> find_child(KeyHash, Keys, Children)
  end.
  
find_child_adj(_, [], [Child]) ->
  {last, {Child, undefined}};
  
find_child_adj(KeyHash, [Key|Keys], [Child,RightAdj|Children]) ->
  if
    KeyHash =< Key -> {Key, {Child, RightAdj}};
    true -> find_child_adj(KeyHash, Keys, [RightAdj|Children])
  end.

split_child(_, empty, Child = #node{m=M,keys=Keys,children=Children}, Tree=#dmerkle{file=File,block=BlockSize}) ->
  {PreLeftKeys, RightKeys} = lists:split((M div 2), Keys),
  {LeftChildren, RightChildren} = lists:split(M div 2, Children),
  [LeftKeyHash| ReversedLeftKeys] = lists:reverse(PreLeftKeys),
  LeftKeys = lists:reverse(ReversedLeftKeys),
  % error_logger:info_msg("splitchild(empty rightkeys ~p rightchildren ~p leftkeys ~p leftchildren ~p~n", [length(RightKeys), length(RightChildren), length(LeftKeys), length(LeftChildren)]),
  {Left, Tree2} = write(#node{m=length(LeftKeys),keys=LeftKeys,children=LeftChildren},Tree),
  {Right, Tree3} = write(Child#node{m=length(RightKeys),keys=RightKeys,children=RightChildren}, Tree2),
  write(#node{m=1,
    keys=[LeftKeyHash],
    children=[{hash(Left),offset(Left)},{hash(Right),offset(Right)}]}, Tree3);

split_child(Parent = #node{keys=Keys,children=Children}, ToReplace, Child = #leaf{values=Values,m=M}, Tree) ->
  % error_logger:info_msg("splitting leaf with offset~p parent with offset~p ~n", [Child#leaf.offset, Parent#node.offset]),
  {LeftValues, RightValues} = lists:split(M div 2, Values),
  % {LeftValues, RightValues} = lists:partition(fun({Hash,_,_}) ->
  %     Hash =< KeyHash
  %   end, Values),
  % error_logger:info_msg("split_child(leaf left ~p right ~p orig ~p~n", [length(LeftValues), length(RightValues), length(Values)]),
  % error_logger:info_msg("lhas ~p rhas ~p orighas ~p~n", [lists:keymember(3784569674, 1, LeftValues), lists:keymember(3784569674, 1, RightValues), lists:keymember(3784569674, 1, Values)]),
  {Left, Tree2} = write(#leaf{m=length(LeftValues),values=LeftValues}, Tree),
  {Right, Tree3} = write(Child#leaf{m=length(RightValues),values=RightValues}, Tree2),
  write(replace(Parent, ToReplace, Left, Right, last_key(Left)), Tree3);
  
split_child(Parent = #node{keys=Keys,children=Children}, ToReplace, Child = #node{m=M,keys=ChildKeys,children=ChildChildren}, Tree) ->
  % error_logger:info_msg("splitting node ~p~n", [Parent]),
  % KeyHash = lists:nth(M div 2, Keys),
  {PreLeftKeys, RightKeys} = lists:split(M div 2, ChildKeys),
  {LeftChildren, RightChildren} = lists:split(M div 2, ChildChildren),
  [LeftKeyHash| ReversedLeftKeys] = lists:reverse(PreLeftKeys),
  LeftKeys = lists:reverse(ReversedLeftKeys),
  % error_logger:info_msg("split_child(node rightkeys ~p rightchildren ~p leftkeys ~p leftchildren ~p~n", [length(RightKeys), length(RightChildren), length(LeftKeys), length(LeftChildren)]),
  {Left, Tree2} = write(#node{m=length(LeftKeys),keys=LeftKeys,children=LeftChildren}, Tree),
  {Right, Tree3} = write(Child#node{m=length(RightKeys),keys=RightKeys,children=RightChildren}, Tree2),
  write(replace(Parent, ToReplace, Left, Right, LeftKeyHash), Tree3).

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

create_or_read_root(Tree = #dmerkle{file=File,block=BlockSize,rootpointer=0}) ->
  {Root, Tree2} = write(#leaf{}, Tree),
  update_root(Tree2, Root);
  
create_or_read_root(Tree = #dmerkle{file=File,block=BlockSize,rootpointer=Ptr}) ->
  Root = read(File, Ptr, BlockSize),
  Tree#dmerkle{root=Root}.

update_root(Tree = #dmerkle{}, Root) ->
  Offset = offset(Root),
  Tree2 = Tree#dmerkle{rootpointer=Offset,root=Root},
  write_header(Tree2),
  Tree2.

write_header(Tree = #dmerkle{file=File}) ->
  {ok, 0} = block_server:write_block(File,0,serialize_header(Tree)),
  Tree.

%this will try and match the current version, if it doesn't then we gotta punch out
deserialize_header(<<?VERSION:8, BlockSize:32, FreePtr:64, RootPtr:64, KP1:64, KP2:64, KP3:64, KP4:64, KP5:64, _Reserved:64/binary>>, Tree) ->
  Tree#dmerkle{block=BlockSize,d=d_from_blocksize(BlockSize),freepointer=FreePtr,rootpointer=RootPtr, fp1=KP1, fp2=KP2, fp3=KP3, fp4=KP4, fp5=KP5};

%hit the canopy
deserialize_header(BinHeader, _) ->
  case BinHeader of
    <<Version:8, _/binary>> -> {error, ?fmt("Mismatched version.  Cannot read version ~p", [Version])};
    _ -> {error, "Cannot read version.  Dmerkle is corrupted."}
  end.

serialize_header(#dmerkle{block=BlockSize, freepointer=FreePtr, root=undefined, rootpointer=RootPtr, fp1=KP1, fp2=KP2, fp3=KP3, fp4=KP4, fp5=KP5}) ->
  FreeSpace = 64*8,
  <<?VERSION:8, BlockSize:32, FreePtr:64, RootPtr:64, KP1:64, KP2:64, KP3:64, KP4:64, KP5:64, 0:FreeSpace>>;

serialize_header(#dmerkle{block=BlockSize, freepointer=FreePtr, root=Root, fp1=KP1, fp2=KP2, fp3=KP3, fp4=KP4, fp5=KP5}) ->
  FreeSpace = 64*8,
  RootPtr = offset(Root),
  <<?VERSION:8, BlockSize:32, FreePtr:64, RootPtr:64, KP1:64, KP2:64, KP3:64, KP4:64, KP5:64, 0:FreeSpace>>.

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
  #leaf{m=M,values=Values,offset=Offset};
  
deserialize(<<3:8, Pointer:64, _/binary>>, Offset) ->
  #free{offset=Offset,pointer=Pointer}.
  
serialize(Free = #free{pointer=Pointer}, BlockSize) ->
  LeftOverBits = (BlockSize - 9) * 8,
  <<3:8,Pointer:64,0:LeftOverBits>>;
  
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
  
  
write(Node, Tree = #dmerkle{file=File,block=BlockSize}) ->
  Offset = offset(Node),
  Bin = serialize(Node, BlockSize),
  {Offset2, Tree2} = take_free_offset(Offset, Tree),
  {ok, NewOffset} = block_server:write_block(File,Offset2,Bin),
  {offset(Node, NewOffset), Tree2}.
  
take_free_offset(eof, Tree = #dmerkle{file=File,block=BlockSize,freepointer=FreePtr}) when FreePtr > 0 ->
  Offset = FreePtr,
  #free{pointer=NewFreePointer} = read(File, FreePtr, BlockSize),
  {Offset, write_header(Tree#dmerkle{freepointer=NewFreePointer})};
  
take_free_offset(Offset, Tree) ->
  {Offset, Tree}.
  
read(File, 0, BlockSize) ->
  throw("tried to read a node from the null pointer");
  
read(File, Offset, BlockSize) ->
  case block_server:read_block(File, Offset, BlockSize) of
    {ok, Bin} -> deserialize(Bin, Offset);
    eof -> error_logger:info_msg("hit an eof for offset ~p", [Offset]),
      undefined;
    {error, Reason} -> error_logger:info_msg("error ~p at offset", [Reason, Offset]),
      undefined
  end.
  
ref_equals(#node{offset=Offset}, #node{offset=Offset}) -> true;
ref_equals(#leaf{offset=Offset}, #leaf{offset=Offset}) -> true;
ref_equals(_, _) -> false.
  
offset(#leaf{offset=Offset}) -> Offset;
offset(#free{offset=Offset}) -> Offset;
offset(#node{offset=Offset}) -> Offset.

offset(Leaf = #leaf{}, Offset) -> Leaf#leaf{offset=Offset};
offset(Free = #free{}, Offset) -> Free#free{offset=Offset};
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
