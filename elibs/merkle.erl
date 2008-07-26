%%%-------------------------------------------------------------------
%%% File:      untitled.erl
%%% @author    Cliff Moon <> []
%%% @copyright 2008 Cliff Moon
%%% @doc  
%%%
%%% @end  
%%%
%%% @since 2008-07-22 by Cliff Moon
%%%-------------------------------------------------------------------
-module(merkle).
-author('cliff moon').

-record(root, {min, max, node}).
-record(node, {hash, middle, left, right}).
-record(leaf, {hash, key}).

%% API
-export([create/2, update/3, delete/2, leaf_size/1, key_diff/2]).

-ifdef(TEST).
-include("etest/merkle_test.erl").
-endif.

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% @spec 
%% @doc
%% @end 
%%--------------------------------------------------------------------

create(Min, Max) ->
  #root{
    min=Min,
    max=Max,
    node=#node{
      hash=empty,
      middle=(Min+Max) div 2,
      left=empty,
      right=empty}}.
  
delete(Key, Root = #root{max=Max,min=Min,node=Node}) ->
  Root#root{node=delete(erlang:phash2(Key), Key, Node)}.
  
delete(KeyHash, Key, Node = #node{left=Left,right=Right,middle=Middle}) ->
  {NewLeft,NewRight} = if
    KeyHash < Middle -> {delete(KeyHash,Key,Left),Right};
    true -> {Left,delete(KeyHash,Key,Left)}
  end,
  Node#node{left=NewLeft,right=NewRight};
  
delete(KeyHash, Key, #leaf{key=Key}) -> empty;

%the key isn't here, so tree is unchanged
delete(KeyHash, Key, Leaf = #leaf{}) -> Leaf.
  
update(Key, Value, Root = #root{max=Max,min=Min,node=Node}) ->
  Root#root{node=update(erlang:phash2(Key), Key, Value, Min, Max, Node)}.
  
update(KeyHash, Key, Value, Min, Max, Node = #node{middle=Middle}) ->
  % error_logger:info_msg("update node ~p with keyhash ~p min ~p max ~p~n", [Node, KeyHash, Min, Max]),
  KeyHash = hash(Key),
  {Left,Right} = if
    KeyHash < Middle -> 
      {update(KeyHash, Key, Value, Min, Middle, Node#node.left), Node#node.right};
    true ->
      {Node#node.left, update(KeyHash, Key, Value, Middle, Max, Node#node.right)}
  end,
  Node#node{
    left=Left,
    right=Right,
    hash=hash({hash(Left),hash(Right)})
  };
  
update(KeyHash, Key, Value, Min, Max, empty) ->
  % error_logger:info_msg("update empty~n", []),
  #leaf{
    hash=hash(Value),
    key=Key
  };

%replace leaf
update(_, Key, Value, _, _, #leaf{key=Key}) ->
  % error_logger:info_msg("replacing leaf ~n", []),
  #leaf{
   hash=hash(Value),
   key=Key
  };

update(KeyHash, Key, Value, Min, Max, Leaf = #leaf{}) ->
  % error_logger:info_msg("update leaf ~p min ~p max ~p ~n", [Leaf, Min, Max]),
  Middle = (Min+Max) div 2,
  Node = #node {middle=Middle,left=empty,right=empty,hash=empty},
  SemiNode = case hash(Leaf#leaf.key) of
    LeafKeyHash when LeafKeyHash < Middle -> Node#node{left=Leaf};
    _ -> Node#node{right=Leaf}
  end,
  update(KeyHash, Key, Value, Min, Max, SemiNode).
  
leaf_size(#root{node=Node}) -> 
  leaf_size(Node);
  
leaf_size(#node{left=Left,right=Right}) ->
  leaf_size(Left) + leaf_size(Right);
  
leaf_size(#leaf{}) -> 1;

leaf_size(empty) -> 0.

key_diff(#root{node=NodeA}, #root{node=NodeB}) ->
  key_diff(NodeA, NodeB);
  
key_diff(NodeLeft = #node{hash=HashA,left=LeftA,right=RightA}, NodeRight = #node{hash=HashB,left=LeftB,right=RightB}) ->
  error_logger:info_msg("diff node ~p~n node ~p~n", [NodeLeft, NodeRight]),
  if
    HashA == HashB -> [];
    true -> key_diff(LeftA,LeftB) ++ key_diff(RightA,RightB)
  end;
  
key_diff(Node = #node{left=Left,right=Right,middle=Middle}, Leaf = #leaf{key=Key}) ->
  error_logger:info_msg("diff node ~p~n leaf ~p~n", [Node, Leaf]),
  case hash(Key) of
    Hash when Hash < Middle -> key_diff(Left, Leaf) ++ key_diff(empty, Right);
    _ -> key_diff(Right, Leaf) ++ key_diff(empty, Left)
  end;
  
key_diff(Leaf = #leaf{key=Key}, Node = #node{left=Left,right=Right,middle=Middle}) ->
  error_logger:info_msg("diff leaf ~p~n node ~p~n", [Leaf, Node]),
  case hash(Key) of
    Hash when Hash < Middle -> key_diff(Leaf, Left) ++ key_diff(empty, Right);
    _ -> key_diff(Leaf, Right) ++ key_diff(empty, Left)
  end;
  
key_diff(Left = #leaf{hash=HashA,key=KeyA}, Right = #leaf{hash=HashB,key=KeyB}) ->
  error_logger:info_msg("diff leaf ~p~n leaf ~p~n", [Left, Right]),
  if
    (HashA == HashB) and (KeyA == KeyB) -> [];
    (HashA /= HashB) and (KeyA == KeyB) -> [KeyA];
    %we're just totally boned here
    true -> [KeyA,KeyB]
  end;

key_diff(empty, empty) -> [];

key_diff(empty, Right = #leaf{key=Key}) -> 
  error_logger:info_msg("diff empty right ~p~n", [Right]),
  [Key];

key_diff(Left = #leaf{key=Key}, empty) -> 
  error_logger:info_msg("diff left ~p empty~n", [Left]),
  [Key];

key_diff(empty, Node = #node{left=Left,right=Right}) ->
  error_logger:info_msg("diff empty right ~p~n", [Node]),
  key_diff(empty, Left) ++ key_diff(empty, Right);
  
key_diff(Node = #node{left=Left,right=Right}, empty) ->
  error_logger:info_msg("diff left ~p empty~n", [Node]),
  key_diff(Left, empty) ++ key_diff(Right, empty).

%%====================================================================
%% Internal functions
%%====================================================================

hash(#root{node=Node}) -> hash(Node);
hash(#node{hash=Hash}) -> Hash;
hash(#leaf{hash=Hash}) -> Hash;
hash(N) -> erlang:phash2(N).