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

-record(dmerkle, {tree,root}).

-include("common.hrl").
-include("dmerkle.hrl").

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
  
get_state(Tree) ->
  gen_server:call(Tree, get_state).

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
  case dmtree:start_link(FileName, BlockSize) of
    {ok, Tree} -> 
      Root = dmtree:root(Tree),
      {ok, #dmerkle{tree=Tree,root=Root}};
    {error, Reason} -> {stop, Reason}
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
handle_call(root, _From, DM = #dmerkle{root=Root}) ->
  {reply, Root, DM};

handle_call({count_trace, Key}, _From, DM = #dmerkle{tree=Tree,root=Root}) ->
  Reply = count_trace(Tree, Root, hash(Key)),
  {reply, Reply, DM};
  
handle_call({count, Level}, _From, DM = #dmerkle{tree=Tree,root=Root}) ->
  Reply = count(Tree, Root, Level),
  {reply, Reply, DM};
  
handle_call(leaves, _From, DM = #dmerkle{root=Root,tree=Tree}) ->
  Reply = leaves(Root, Tree, []),
  {reply, Reply, DM};
  
handle_call({update, Key, Value}, _From, DM = #dmerkle{tree=Tree,root=Root}) ->
  % error_logger:info_msg("inserting ~p~n", [Key]),
  % dmtree:tx_begin(Tree),
  D = dmtree:d(Tree),
  M = m(Root),
  NewTree = if
    M >= D-1 -> %allocate new root, move old root and split
      % ?infoMsg("root M is larged than D-1.  splitting.~n"),
      Root2 = split_child(#node{}, empty, Root, Tree),
      dmtree:update_root(Root2, Tree),
      % error_logger:info_msg("found: ~p~n", [visualized_find("key60", Tree#dmerkle{root=FinalRoot})]),
      Root3 = update(hash(Key), Key, Value, Root2, Tree),
      DM#dmerkle{root=Root3};
    true -> 
      Root2 = update(hash(Key), Key, Value, Root, Tree),
      % ?infoFmt("updated root ~p~n", [Root2]),
      DM#dmerkle{root=Root2}
  end,
  % dmtree:tx_commit(Tree),
  {reply, self(), NewTree};
  
handle_call({find, Key}, _From, DM = #dmerkle{root=Root,tree=Tree}) ->
  Reply = find(hash(Key), Key, Root, Tree),
  {reply, Reply, DM};
  
handle_call(hash, _From, DM = #dmerkle{root=Root,tree=Tree}) ->
  {reply, hash(Root), DM};
  
handle_call({visualized_find, Key}, _From, DM = #dmerkle{root=Root,tree=Tree}) ->
  Reply = visualized_find(hash(Key), Key, Root, Tree, []),
  {reply, Reply, DM};
  
handle_call({delete, Key}, _From, DM = #dmerkle{tree=Tree,root=Root}) ->
  % dmtree:tx_begin(Tree),
  RetNode = delete(hash(Key), Key, root, Root, Tree),
  % dmtree:tx_commit(Tree),
  {reply, self(), DM#dmerkle{root=RetNode}};
  
handle_call(blocksize, _From, DM = #dmerkle{tree=Tree}) ->
  {reply, dmtree:block_size(Tree), DM};
  
handle_call(get_tree, _From, DM = #dmerkle{tree=Tree}) ->
  {reply, Tree, DM};
  
handle_call(get_state, _From, DM) ->
  {reply, DM, DM};
  
handle_call({key_diff, PidB}, _From, DMA = #dmerkle{root=RootA,tree=TreeA}) ->
  Reply = if
    self() == PidB -> {error, "Cannot do a diff on the same merkle tree."};
    true ->
      TreeB = gen_server:call(PidB, get_tree),
      RootB = gen_server:call(PidB, root),
      {KeysA, KeysB} = key_diff(RootA, RootB, TreeA, TreeB, [], []),
      lists:usort(diff_merge(TreeA, TreeB, lists:keysort(1, KeysA), lists:keysort(1, KeysB), []))
  end,
  {reply, Reply, DMA};
  
handle_call(filename, _From, DM = #dmerkle{tree=Tree}) ->
  {reply, dmtree:filename(Tree), DM};
  
handle_call(scan_for_empty, _From, DM = #dmerkle{root=Root,tree=Tree}) ->
  Reply = scan_for_empty(Tree, Root),
  {reply, Reply, DM}.

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
terminate(_Reason, #dmerkle{tree=Tree}) ->
    dmtree:stop(Tree).

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
delete(KeyHash, Key, Parent, Node = #node{children=Children,keys=Keys}, Tree) ->
  % error_logger:info_msg("delete key ~p keyhash ~p from node ~p~n", [Key, KeyHash, Node]),
  {WhatItDo, DeleteNode} = case find_child_adj(KeyHash, Keys, Children) of
    {FoundKey, {{LeftHash, LeftPointer}, {RightHash, RightPointer}}} ->
      LeftNode = dmtree:read(LeftPointer, Tree),
      RightNode = dmtree:read(RightPointer, Tree),
      % ?infoFmt("delete_merge foundkey ~p~nLeftPointer ~p~nrightpointer ~p~nparent ~p~nnode ~p~nleftnode ~p~nrightnode ~p~n", [FoundKey, LeftPointer, RightPointer, Parent, Node, LeftNode, RightNode]),
      delete_merge(FoundKey, dmtree:d(Tree), Parent, Node, LeftNode, RightNode, Tree);
    {FoundKey, {{LeftHash, LeftPointer}, undefined}} ->
      {nothing, dmtree:read(LeftPointer, Tree)}
  end,
  % ?infoMsg("recursing into delete~n"),
  ReturnNode = delete(KeyHash, Key, Node, DeleteNode, Tree),
  % ?infoFmt("reduced from delete ~p~n", [ReturnNode]),
  Eqls = ref_equals(ReturnNode, Node),
  case WhatItDo of
    wamp_wamp -> ReturnNode;
    _ when Eqls -> ReturnNode;
    _ -> update_hash(hash(ReturnNode), offset(ReturnNode), Node, Tree)
  end;

delete(KeyHash, Key, Parent, Leaf = #leaf{values=Values}, Tree) ->
  % error_logger:info_msg("delete key ~p keyhash ~p from ~p~n", [Key, KeyHash, Leaf]),
  case lists:keytake(KeyHash, 1, Values) of
    {value, {KeyHash,Pointer,ValHash}, NewValues} ->
      NewLeaf = Leaf#leaf{values=NewValues,m=length(NewValues)},
      % ?infoFmt("new leaf ~p~n", [NewLeaf]),
      dmtree:delete_key(Pointer, Key, Tree),
      dmtree:write(NewLeaf, Tree);
    false -> 
      % ?infoFmt("couldnt find ~p in ~p~n", [KeyHash, Leaf]),
      Leaf
  end.

update_hash(Hash, Pointer, Node = #node{children=Children}, Tree) ->
  % ?infoFmt("updating hash,ptr ~p for ~p~n", [{Hash,Pointer}, Node]),
  NewNode = Node#node{children=lists:keyreplace(Pointer, 2, Children, {Hash,Pointer})},
  % ?infoFmt("updated node ~p~n", [NewNode]),
  dmtree:write(NewNode, Tree).

% delete_merge(FoundKey,
%              Parent = #node{keys=PKeys,children=PChildren,m=M})

% we have to replace the parent in this case with the merged leaf
%%merging leaves
delete_merge(FoundKey, D,
             root,
             Root = #node{keys=PKeys,children=PChildren,m=PM},
             LeftLeaf = #leaf{values=LeftValues,m=LeftM},
             RightLeaf = #leaf{values=RightValues,m=RightM},
             Tree) when (LeftM+RightM) =< D, PM == 1 ->
  % ?infoMsg("Replacing root merging leaves~n"),
  dmtree:delete(Root#node.offset, Tree),
  dmtree:delete(RightLeaf#leaf.offset, Tree),
  NewLeaf = dmtree:write(LeftLeaf#leaf{m=LeftM+RightM,values=LeftValues++RightValues}, Tree),
  dmtree:update_root(NewLeaf, Tree),
  {wamp_wamp, NewLeaf};
  
delete_merge(FoundKey, D,
             SuperParent = #node{children=SPChildren},
             Parent = #node{keys=PKeys,children=PChildren,m=PM},
             LeftLeaf = #leaf{values=LeftValues,m=LeftM},
             RightLeaf = #leaf{values=RightValues,m=RightM},
             Tree) when (LeftM+RightM) =< D, length(PKeys) == 1 ->
  % ?infoMsg("Replacing node merging leaves~n"),
  dmtree:delete(Parent#node.offset, Tree),
  dmtree:delete(RightLeaf#leaf.offset, Tree),
  NewLeaf = dmtree:write(LeftLeaf#leaf{m=LeftM+RightM,values=LeftValues++RightValues}, Tree),
  NP = dmtree:write(SuperParent#node{children=lists:keyreplace(offset(Parent), 2, PChildren, {hash(NewLeaf),offset(NewLeaf)})}, Tree),
  % ?infoFmt("replaced ~p in super parent ~p~n", [{hash(Parent),offset(Parent)}, NP]),
  {wamp_wamp, NewLeaf};
  
delete_merge(FoundKey, D,
             SuperParent,
             Parent = #node{keys=PKeys,children=PChildren,m=PM}, 
             LeftLeaf = #leaf{values=LeftValues,m=LeftM}, 
             RightLeaf = #leaf{values=RightValues,m=RightM}, 
             Tree) when (LeftM+RightM) =< D ->
  %we can merge within reqs gogogo
  % ?infoMsg("Merging leaves~n"),
  N = if
    FoundKey == last -> length(PKeys);
    true -> lib_misc:position(FoundKey, PKeys)
  end,
  % ?infoFmt("FoundKey ~p~nPKeys ~p~nPChildren ~p~nN ~p~nLeftM ~p~nRightM ~p~nD ~p~n", [FoundKey, PKeys, PChildren, N, LeftM, RightM, D]),
  NP = dmtree:write(remove_nth(Parent, N), Tree),
  dmtree:write(LeftLeaf#leaf{m=LeftM+RightM,values=LeftValues++RightValues}, Tree),
  dmtree:delete(RightLeaf#leaf.offset, Tree),
  % ?infoFmt("new parent: ~p~n", [NP]),
  { merge, NP};
  
%merging nodes
delete_merge(FoundKey, D,
             root,
             Root = #node{keys=PKeys,children=PChildren,m=PM},
             LeftNode = #node{m=LeftM},
             RightNode = #node{m=RightM},
             Tree) when (LeftM+RightM) < D, length(PKeys) == 1 ->
  % ?infoMsg("replacing root merging nodes~n"),
  NC = dmtree:write(merge_nodes(FoundKey, PKeys, LeftNode, RightNode), Tree),
  dmtree:delete(offset(Root), Tree),
  dmtree:delete(offset(RightNode), Tree),
  dmtree:update_root(NC, Tree),
  {wamp_wamp, NC};
  
delete_merge(FoundKey, D,
             SuperParent = #node{children=SPChildren},
             Parent = #node{keys=PKeys,children=PChildren,m=PM},
             LeftNode = #node{m=LeftM,keys=LeftKeys,children=LeftChildren},
             RightNode = #node{m=RightM,keys=RightKeys,children=RightChildren},
             Tree) when (LeftM+RightM) < D, length(PKeys) == 1 ->
  % ?infoMsg("Replacing node merging nodes~n"),
  dmtree:delete(offset(RightNode), Tree),
  dmtree:delete(offset(Parent), Tree),
  ParentPointer = offset(Parent),
  ParentHash = hash(Parent),
  NN = merge_nodes(FoundKey, PKeys, LeftNode, RightNode),
  NP = dmtree:write(SuperParent#node{children=lists:keyreplace(offset(Parent), 2, PChildren, {hash(NN),offset(NN)})}, Tree),
  % ?infoFmt("NN: ~p~n", [NN]),
  NewNode = dmtree:write(NN, Tree),
  {wamp_wamp, NewNode};

delete_merge(FoundKey, D,
             SuperParent,
             Parent = #node{keys=PKeys,children=PChildren,m=PM},
             LeftNode = #node{keys=LeftKeys,children=LeftChildren,m=LM},
             RightNode = #node{keys=RightKeys,children=RightChildren,m=RM},
             Tree)  when (LM+RM) < D ->
  % ?infoMsg("merging nodes~n"),
  N = if
    FoundKey == last -> length(PKeys) -1;
    true -> lib_misc:position(FoundKey, PKeys)
  end,
  % ?infoFmt("FoundKey ~p~nPKeys ~p~nPChildren ~p~nN ~p~nLeftM ~p~nRightM ~p~nD ~p~n", [FoundKey, PKeys, PChildren, N, LM, RM, D]),
  NP = dmtree:write(remove_nth(Parent, N), Tree),
  NC = dmtree:write(merge_nodes(FoundKey, PKeys, LeftNode, RightNode), Tree),
  dmtree:delete(RightNode#node.offset, Tree),
  % ?infoFmt("new child: ~p~n", [NC]),
  % ?infoFmt("new parent: ~p~n", [NP]),
  {merge, NP};
  
delete_merge(last, _, _, _, _, Right, Tree) ->
  % ?infoMsg("not merging~n"),
  {nothing, Right};
  
delete_merge(_, _, _, _, Left, _, Tree) ->
  %merged leaf is too large, do not merge
  % ?infoMsg("not merging~n"),
  {nothing, Left}.
    
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


count_trace(_, #leaf{values=Values}, Hash) ->
  length(lists:filter(fun({H, _, _}) -> 
      H == Hash
    end, Values));
    
count_trace(Tree, #node{children=Children}, Hash) ->
  Counts = lists:reverse(lists:foldl(fun({_, Ptr}, Acc) ->
      Child = dmtree:read(Ptr, Tree),
      [count_trace(Tree, Child, Hash) | Acc]
    end, [], Children)).

count(_, _, 0) ->
  1;
  
count(Tree, Node = #node{children=Children}, Level) ->
  lists:foldl(fun({_,Ptr}, Acc) ->
      Child = dmtree:read(Ptr, Tree),
      Acc + count(Tree, Child, Level-1)
    end, 0, Children);
    
count(#dmerkle{}, #leaf{values=Values}, _) ->
  length(Values).

diff_merge(TreeA, TreeB, [], [], Ret) ->
  lists:sort(Ret);

diff_merge(TreeA, TreeB, [{Hash,Ptr,_}|KeysA], [], Ret) ->
  Key = dmtree:read_key(Ptr, TreeA),
  if
    Key == eof -> error_logger:info_msg("Key is eof: ([{~p, ~p}|KeysA], [], Ret)~n", [Hash, Ptr]);
    true -> ok
  end,
  diff_merge(TreeA, TreeB, KeysA, [], [Key|Ret]);

diff_merge(TreeA, TreeB, [], [{Hash,Ptr,_}|KeysB], Ret) ->
  Key = dmtree:read_key(Ptr, TreeB),
  if
    Key == eof -> error_logger:info_msg("Key is eof: ([], [{~p, ~p}|KeysB], Ret)~n", [Hash, Ptr]);
    true -> ok
  end,
  diff_merge(TreeA, TreeB, [], KeysB, [Key|Ret]);

diff_merge(TreeA, TreeB, [{Hash,PtrA,Val}|KeysA], [{Hash,PtrB,Val}|KeysB], Ret) ->
  diff_merge(TreeA, TreeB, KeysA, KeysB, Ret);

diff_merge(TreeA, TreeB, [{Hash,PtrA,_}|KeysA], [{Hash,PtrB,_}|KeysB], Ret) ->
  Key = dmtree:read_key(PtrA, TreeA),
  if
    Key == eof -> error_logger:info_msg("Key is eof: ([{~p, ~p}|KeysA], [{~p, ~p}|KeysB], Ret)~n", [Hash, PtrA, Hash, PtrB]);
    true -> ok
  end,
  diff_merge(TreeA, TreeB, KeysA, KeysB, [Key|Ret]);

diff_merge(TreeA, TreeB, [{HashA,PtrA,_}|KeysA], [{HashB,PtrB,ValB}|KeysB], Ret) when HashA < HashB ->
  KeyA = dmtree:read_key(PtrA, TreeA),
  diff_merge(TreeA, TreeB, KeysA, [{HashB,PtrB,ValB}|KeysB], [KeyA|Ret]);

diff_merge(TreeA, TreeB, [{HashA,PtrA,ValA}|KeysA], [{HashB,PtrB,_}|KeysB], Ret) when HashA > HashB ->
  KeyB = dmtree:read_key(PtrB, TreeB),
  diff_merge(TreeA, TreeB, [{HashA,PtrA,ValA}|KeysA], KeysB, [KeyB|Ret]).
  
foldl(Fun, Acc, Node = #node{children=Children}, Tree) ->
  Acc2 = Fun(Node, Acc),
  lists:foldl(fun({_,Pointer}, A) ->
      foldl(Fun, A, dmtree:read(Pointer, Tree), Tree)
    end, Acc2, Children);
  
foldl(Fun, Acc, Leaf = #leaf{}, Tree) ->
  Fun(Leaf, Acc).
  
scan_for_empty(Tree, Node = #node{children=Children,keys=Keys}) ->
  if
    length(Keys) == 0 -> io:format("node was empty: ~p", [Node]);
    true -> noop
  end,
  lists:foreach(fun({_, ChldPtr}) -> scan_for_empty(Tree, dmtree:read(ChldPtr, Tree)) end, Children);
  
scan_for_empty(Tree, Leaf = #leaf{values=Values}) ->
  if 
    length(Values) == 0 -> io:format("leaf was empty: ~p", [Leaf]);
    true -> noop
  end;
  
scan_for_empty(_, undefined) ->
  io:format("got an undefined!").
  
scan_for_nulls(Node = #node{children=Children}, Tree) ->
  lists:foreach(fun
    ({_, 0}) ->
      ?infoFmt("has a zero: ~p~n",[Node]);
    ({_, ChldPtr}) ->
      % timer:sleep(1),
      scan_for_nulls(dmtree:read(ChldPtr,Tree), Tree)
    end, Children);
    
scan_for_nulls(#leaf{}, _) ->
  ok;
  
scan_for_nulls(_, _) ->
  ok.

key_diff(_LeafA = #leaf{values=ValuesA}, _LeafB = #leaf{values=ValuesB}, 
    TreeA, TreeB, KeysA, KeysB) ->
  leaf_diff(ValuesA, ValuesB, TreeA, TreeB, KeysA, KeysB);

key_diff(#node{children=ChildrenA}, #node{children=ChildrenB},
    TreeA, TreeB, KeysA, KeysB) ->
  % error_logger:info_msg("node differences ~n"),
  node_diff(ChildrenA, ChildrenB, TreeA, TreeB, KeysA, KeysB);
  
key_diff(Leaf = #leaf{}, Node = #node{children=Children}, TreeA, TreeB, KeysA, KeysB) ->
  % error_logger:info_msg("leaf node differences ~n"),
  lists:foldl(fun({_, Ptr}, {AccA, AccB}) ->
      Child = dmtree:read(Ptr, TreeB),
      key_diff(Leaf, Child, TreeA, TreeB, AccA, AccB)
    end, {KeysA, KeysB}, Children);
  
key_diff(Node = #node{children=Children}, Leaf = #leaf{}, TreeA, TreeB, KeysA, KeysB) ->
  % error_logger:info_msg("node leaf differences  ~n"),
  lists:foldl(fun({_, Ptr}, {AccA, AccB}) ->
      Child = dmtree:read(Ptr, TreeA),
      key_diff(Child, Leaf, TreeA, TreeB, AccA, AccB)
    end, {KeysA, KeysB}, Children).

node_diff([], [], TreeA, TreeB, KeysA, KeysB) -> {KeysA, KeysB};

node_diff([], ChildrenB, TreeA, TreeB, KeysA, KeysB) ->
  % error_logger:info_msg("node_diff empty children ~n"),
  {KeysA, lists:foldl(fun({_, Ptr}, Acc) ->
      Child = dmtree:read(Ptr, TreeB),
      hash_leaves(Child, TreeB, Acc)
    end, KeysB, ChildrenB)};
    
node_diff(ChildrenA, [], TreeA, TreeB, KeysA, KeysB) ->
  % error_logger:info_msg("node_diff children empty ~n"),
  {lists:foldl(fun({_, Ptr}, Acc) ->
      Child = dmtree:read(Ptr, TreeA),
      hash_leaves(Child, TreeA, Acc)
    end, KeysA, ChildrenA), KeysB};
    
node_diff([{Hash,PtrA}|ChildrenA], [{Hash,PtrB}|ChildrenB], TreeA, TreeB, KeysA, KeysB) ->
  % error_logger:info_msg("equal nodes ~n"),
  node_diff(ChildrenA, ChildrenB, TreeA, TreeB, KeysA, KeysB);
  
node_diff([{HashA,PtrA}|ChildrenA], [{HashB,PtrB}|ChildrenB], 
    TreeA, TreeB, KeysA, KeysB) ->
  % error_logger:info_msg("nodes are different ~n"),
  ChildA = dmtree:read(PtrA, TreeA),
  ChildB = dmtree:read(PtrB, TreeB),
  {KeysA1, KeysB1} = key_diff(ChildA, ChildB, TreeA, TreeB, KeysA, KeysB),
  node_diff(ChildrenA, ChildrenB, TreeA, TreeB, KeysA1, KeysB1).

leaf_diff([], [], _, _, KeysA, KeysB) -> {KeysA, KeysB};

leaf_diff([], [{KeyHash, Ptr, Val}|ValuesB], TreeA, TreeB, KeysA, KeysB) ->
  % error_logger:info_msg("leaf_diff empty values ~n"),
  leaf_diff([], ValuesB, TreeA, TreeB, KeysA, [{KeyHash, Ptr, Val}|KeysB]);
  
leaf_diff([{KeyHash,Ptr,Val}|ValuesA], [], TreeA, TreeB, KeysA, KeysB) ->
  % error_logger:info_msg("leaf_diff values empty ~n"),
  leaf_diff(ValuesA, [], TreeA, TreeB, [{KeyHash, Ptr,Val}|KeysA], KeysB);
  
leaf_diff([{Hash, _, Val}|ValuesA], [{Hash, _, Val}|ValuesB], TreeA, TreeB, KeysA, KeysB) ->
  % error_logger:info_msg("leaf_diff equals ~n"),
  leaf_diff(ValuesA, ValuesB, TreeA, TreeB, KeysA, KeysB);
  
leaf_diff([{Hash, PtrA, ValA}|ValuesA], [{Hash, PtrB, ValB}|ValuesB], TreeA, TreeB, KeysA, KeysB) ->
  % error_logger:info_msg("leaf_diff equal keys, diff vals ~n"),
  leaf_diff(ValuesA, ValuesB, TreeA, TreeB, [{Hash,PtrA,ValA}|KeysA], KeysB);
  
leaf_diff([{HashA, PtrA, ValA}|ValuesA], [{HashB, PtrB, ValB}|ValuesB], TreeA, TreeB, KeysA, KeysB) when HashA < HashB ->
  % error_logger:info_msg("leaf_diff complete diff ~p < ~p ~n", [HashA, HashB]),
  leaf_diff(ValuesA, [{HashB, PtrB, ValB}|ValuesB], TreeA, TreeB, [{HashA, PtrA, ValA}|KeysA], KeysB);
  
leaf_diff([{HashA, PtrA, ValA}|ValuesA], [{HashB, PtrB, ValB}|ValuesB], TreeA, TreeB, KeysA, KeysB) when HashA > HashB ->
  % error_logger:info_msg("leaf_diff complete diff ~p > ~p ~n", [HashA, HashB]),
  leaf_diff([{HashA, PtrA, ValA}|ValuesA], ValuesB, TreeA, TreeB, KeysA, [{HashB, PtrB, ValB}|KeysB]).

hash_leaves(#node{children=Children}, Tree, Keys) ->
  lists:foldl(fun({_,Ptr}, Acc) ->
      Node = dmtree:read(Ptr, Tree),
      hash_leaves(Node, Tree, Acc)
    end, Keys, Children);

hash_leaves(#leaf{values=Values}, Tree, Keys) ->
  lists:foldl(fun({KeyHash, Ptr, ValHash}, Acc) ->
      [{KeyHash, Ptr, ValHash}|Acc]
    end, Keys, Values).

leaves(#node{children=Children}, Tree, Keys) ->
  lists:foldl(fun({_,Ptr}, Acc) ->
      Node = dmtree:read(Ptr, Tree),
      leaves(Node, Tree, Acc)
    end, Keys, Children);
    
leaves(#leaf{values=Values}, Tree, Keys) ->
  lists:foldl(fun({KeyHash, Ptr, ValHash}, Acc) ->
      Key = dmtree:read_key(Ptr, Tree),
      [{Key, ValHash}|Acc]
    end, Keys, Values).

visualized_find(KeyHash, Key, Node = #node{keys=Keys,children=Children}, Tree, Trail) ->
  {FoundKey, {_,ChildPointer}} = find_child(KeyHash, Keys, Children),
  {Before, After} = lists:partition(fun(E) -> E =< FoundKey end, Keys),
  % error_logger:info_msg("finding keyhash ~p in ~p got ~p~n", [KeyHash, Keys, _FoundKey]),
  visualized_find(KeyHash, Key, dmtree:read(ChildPointer,Tree), Tree, [{FoundKey, offset(Node), length(Before)} | Trail]);

visualized_find(KeyHash, Key, Leaf = #leaf{values=Values}, Tree, Trail) ->
  error_logger:info_msg("looking for ~p in ~p~n", [KeyHash, Values]),
  case lists:keysearch(KeyHash, 1, Values) of
    {value, {KeyHash,_,ValHash}} -> 
      error_logger:info_msg("find trail for ~p(~p): ~p~n", [Key, KeyHash, lists:reverse([{KeyHash, offset(Leaf)} | Trail])]),
      ValHash;
    false -> 
      error_logger:info_msg("find trail for ~p(~p): ~p~n", [Key, KeyHash, lists:reverse(Trail)]),
      not_found
  end.

find(KeyHash, Key, Node = #node{keys=Keys,children=Children}, Tree) ->
  {_FoundKey, {_,ChildPointer}} = find_child(KeyHash, Keys, Children),
  % error_logger:info_msg("finding keyhash ~p in ~p got ~p~n", [KeyHash, Keys, _FoundKey]),
  find(KeyHash, Key, dmtree:read(ChildPointer,Tree), Tree);
  
find(KeyHash, Key, Leaf = #leaf{values=Values}, Tree) ->
  % error_logger:info_msg("looking for ~p in ~p~n", [KeyHash, Values]),
  case lists:keysearch(KeyHash, 1, Values) of
    {value, {KeyHash,_,ValHash}} -> ValHash;
    false -> not_found
  end.

update(KeyHash, Key, Value, Node = #node{children=Children,keys=Keys}, Tree) ->
  % error_logger:info_msg("update(~p, ~p, Value, #node ~p, Tree)~n", [KeyHash, Key, offset(Node)]),
  D = dmtree:d(Tree),
  {FoundKey, {ChildHash,ChildPointer}} = find_child(KeyHash, Keys, Children),
  if
    ChildPointer == 0 -> error_logger:info_msg("reading child at ~p~n for node with M ~p keys ~p children ~p~n", [ChildPointer, m(Node), length(Keys), length(Children)]);
    true -> ok
  end,
  Child = dmtree:read(ChildPointer,Tree),
  case m(Child) of
    M when M >= D -> 
      Node2 = split_child(Node, FoundKey, Child, Tree),
      update(KeyHash, Key, Value, Node2, Tree);
    _ -> 
      Child2 = update(KeyHash, Key, Value, Child, Tree),
      dmtree:write(Node#node{m=length(Node#node.keys),children=lists:keyreplace(ChildPointer, 2, Children, {hash(Child2),offset(Child2)})}, Tree)
  end;
  
update(KeyHash, Key, Value, Leaf = #leaf{values=Values}, Tree) ->
  % error_logger:info_msg("update(~p, ~p, Value, #leaf ~p, Tree)~n", [KeyHash, Key, offset(Leaf)]),
  NewValHash = hash(Value),
  case lists:keysearch(KeyHash, 1, Values) of
    {value, {KeyHash,Pointer,ValHash}} ->
      case dmtree:read_key(Pointer, Tree) of
        Key ->
          dmtree:write(Leaf#leaf{values=lists:keyreplace(KeyHash, 1, Values, {KeyHash,Pointer,NewValHash})}, Tree);
        _ ->  %we still need to deal with collision here
          % error_logger:info_msg("hash found but no key found, inserting new ~n"),
          NewPointer = dmtree:write_key(eof, Key, Tree),
          dmtree:write(Leaf#leaf{values=lists:keymerge(1, Values, [{KeyHash,NewPointer,NewValHash}])}, Tree)
      end;
    false ->
      % error_logger:info_msg("no hash or key found, inserting new ~n"),
      NewPointer = dmtree:write_key(eof, Key, Tree),
      NewValues = lists:keymerge(1, Values, [{KeyHash,NewPointer,NewValHash}]),
      dmtree:write(Leaf#leaf{m=length(NewValues),values=NewValues}, Tree)
  end.
  
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

split_child(_, empty, Child = #node{m=M,keys=Keys,children=Children}, Tree) ->
  {PreLeftKeys, RightKeys} = lists:split((M div 2), Keys),
  {LeftChildren, RightChildren} = lists:split(M div 2, Children),
  [LeftKeyHash| ReversedLeftKeys] = lists:reverse(PreLeftKeys),
  LeftKeys = lists:reverse(ReversedLeftKeys),
  % error_logger:info_msg("splitchild(empty rightkeys ~p rightchildren ~p leftkeys ~p leftchildren ~p~n", [length(RightKeys), length(RightChildren), length(LeftKeys), length(LeftChildren)]),
  Left = dmtree:write(#node{m=length(LeftKeys),keys=LeftKeys,children=LeftChildren},Tree),
  Right = dmtree:write(Child#node{m=length(RightKeys),keys=RightKeys,children=RightChildren}, Tree),
  dmtree:write(#node{m=1,
    keys=[LeftKeyHash],
    children=[{hash(Left),offset(Left)},{hash(Right),offset(Right)}]}, Tree);

split_child(Parent = #node{keys=Keys,children=Children}, ToReplace, Child = #leaf{values=Values,m=M}, Tree) ->
  % error_logger:info_msg("splitting leaf ~p with offset~p parent with offset~p ~n", [Child, Child#leaf.offset, Parent#node.offset]),
  {LeftValues, RightValues} = lists:split(M div 2, Values),
  % error_logger:info_msg("split_child(leaf left ~p right ~p orig ~p~n", [length(LeftValues), length(RightValues), length(Values)]),
  % error_logger:info_msg("lhas ~p rhas ~p orighas ~p~n", [lists:keymember(3784569674, 1, LeftValues), lists:keymember(3784569674, 1, RightValues), lists:keymember(3784569674, 1, Values)]),
  Left = dmtree:write(#leaf{m=length(LeftValues),values=LeftValues}, Tree),
  Right = dmtree:write(Child#leaf{m=length(RightValues),values=RightValues}, Tree),
  dmtree:write(replace(Parent, ToReplace, Left, Right, last_key(Left)), Tree);
  
split_child(Parent = #node{keys=Keys,children=Children}, ToReplace, Child = #node{m=M,keys=ChildKeys,children=ChildChildren}, Tree) ->
  % error_logger:info_msg("splitting node ~p~n", [Parent]),
  {PreLeftKeys, RightKeys} = lists:split(M div 2, ChildKeys),
  {LeftChildren, RightChildren} = lists:split(M div 2, ChildChildren),
  [LeftKeyHash| ReversedLeftKeys] = lists:reverse(PreLeftKeys),
  LeftKeys = lists:reverse(ReversedLeftKeys),
  % error_logger:info_msg("split_child(node rightkeys ~p rightchildren ~p leftkeys ~p leftchildren ~p~n", [length(RightKeys), length(RightChildren), length(LeftKeys), length(LeftChildren)]),
  Left = dmtree:write(#node{m=length(LeftKeys),keys=LeftKeys,children=LeftChildren}, Tree),
  Right = dmtree:write(Child#node{m=length(RightKeys),keys=RightKeys,children=RightChildren}, Tree),
  dmtree:write(replace(Parent, ToReplace, Left, Right, LeftKeyHash), Tree).

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
