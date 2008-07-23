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
-export([create/2, update/3]).

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
  
update(Key, Value, Root = #root{max=Max,min=Min,node=Node}) ->
  Root#root{node=update(erlang:phash2(Key), Key, Value, Min, Max, Node)}.
  
update(KeyHash, Key, Value, Min, Max, Node = #node{middle=Middle}) ->
  error_logger:info_msg("update node ~p with keyhash ~p min ~p max ~p~n", [Node, KeyHash, Min, Max]),
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
  error_logger:info_msg("update empty~n", []),
  #leaf{
    hash=hash(value),
    key=Key
  };

%replace leaf
update(_, Key, Value, _, _, #leaf{key=Key}) ->
  error_logger:info_msg("replacing leaf ~n", []),
  #leaf{
   hash=hash(Value),
   key=Key
  };

update(KeyHash, Key, Value, Min, Max, Leaf = #leaf{}) ->
  error_logger:info_msg("update leaf ~p min ~p max ~p ~n", [Leaf, Min, Max]),
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

%%====================================================================
%% Internal functions
%%====================================================================

hash(#root{node=Node}) -> hash(Node);
hash(#node{hash=Hash}) -> Hash;
hash(#leaf{hash=Hash}) -> Hash;
hash(N) -> erlang:phash2(N).