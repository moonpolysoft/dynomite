-include_lib("eunit.hrl").

% -export([stress/0]).

test_cleanup() ->
  file:delete("/Users/cliff/data/dmerkle.idx"),
  file:delete("/Users/cliff/data/dmerkle.keys"),
  file:delete("/Users/cliff/data/dmerkle1.idx"),
  file:delete("/Users/cliff/data/dmerkle1.keys").

deserialize_node_test() ->
  NodeBin = <<0:8, 2:32, 
    1:32, 2:32, 0:32, 0:32,
    3:32, 4:64, 5:32, 6:64, 7:32, 8:64, 0:32, 0:64, 0:32, 0:64>>,
  Node = deserialize(NodeBin, 20),
  #node{m=2,children=Children,keys=Keys,offset=20} = Node,
  [{3, 4}, {5, 6}, {7, 8}] = Children,
  [1, 2] = Keys.
  
deserialize_leaf_test() ->
  LeafBin = <<1:8, 2:32,
    1:32, 2:64, 3:32,
    4:32, 5:64, 6:32,
    0:352>>,
  Leaf = deserialize(LeafBin, 20),
  #leaf{m=2,values=Values,offset=20} = Leaf,
  [{1, 2, 3}, {4, 5, 6}] = Values.
  
serialize_node_test() ->
  Bin = serialize(#node{
    m=2,
    keys=[1, 2],
    children=[{3, 4}, {5, 6}, {7, 8}]
  }, 81),
  error_logger:info_msg("node bin ~p~n", [Bin]),
  Bin = <<0:8, 2:32, 
    1:32, 2:32, 0:32, 0:32,
    3:32, 4:64, 5:32, 6:64, 7:32, 8:64, 0:192>>.
    
serialize_leaf_test() ->
  Bin = serialize(#leaf{
    m=2,
    values=[{1, 2, 3}, {4, 5, 6}, {7, 8, 9}]
  }, 81),
  error_logger:info_msg("leaf bin ~p~n", [Bin]),
  Bin = <<1:8, 2:32,
    1:32, 2:64, 3:32,
    4:32, 5:64, 6:32,
    7:32, 8:64, 9:32,
    0:224>>.
    
node_round_trip_test() ->
  Node = #node{
    m=2,
    keys=[1, 2],
    children=[{4, 5}, {6, 7}, {8, 9}],
    offset = 0
  },
  Node = deserialize(serialize(Node, 81), 0).
  
leaf_round_trip_test() ->
  Leaf = #leaf{
    m=2,
    values=[{1, 2, 3}, {4, 5, 6}],
    offset=0
  },
  Leaf = deserialize(serialize(Leaf, 81), 0).
  
open_and_close_test() ->
  test_cleanup(),
  Merkle = open("/Users/cliff/data/dmerkle", 256),
  Root = Merkle#dmerkle.root,
  error_logger:info_msg("root ~p~n", [Root]),
  12 = Root#leaf.offset,
  0 = Root#leaf.m,
  close(Merkle).
  
open_and_insert_one_test() ->
  test_cleanup(),
  Tree = update("mykey", <<"myvalue">>, open("/Users/cliff/data/dmerkle", 256)),
  Root = Tree#dmerkle.root,
  error_logger:info_msg("root w/ one ~p merkle~p~n", [Root, Tree]),
  1 = Root#leaf.m,
  Hash = hash(<<"myvalue">>),
  Hash = find("mykey", Tree),
  close(Tree).
  
open_and_reopen_test() ->
  test_cleanup(),
  Tree = update("mykey", <<"myvalue">>, open("/Users/cliff/data/dmerkle", 256)),
  close(Tree),
  NewTree = open("/Users/cliff/data/dmerkle", 256),
  Hash = hash(<<"myvalue">>),
  Hash = find("mykey", NewTree),
  close(NewTree).

open_and_insert_260_test() ->
  open_and_insert_n(260).
  
open_and_insert_1000_test() ->
  test_cleanup(),
  error_logger:info_msg("D: ~p", [d_from_blocksize(256)]),
  Tree = lists:foldl(fun(N, Tree) ->
      update(lists:concat(["key", N]), lists:concat(["value", N]), Tree)
    end, open("/Users/cliff/data/dmerkle", 256), lists:seq(1,1000)),
  true = lists:all(fun(N) -> 
      Hash = hash(lists:concat(["value", N])),
      Result = Hash == find(lists:concat(["key", N]), Tree),
      if
        Result -> Result;
        true -> 
          error_logger:info_msg("could not get ~p was ~p~n", [N, find(lists:concat(["key", N]), Tree)]),
          timer:sleep(1000),
          Result
      end
    end, lists:seq(1, 1000)),
  close(Tree).
  
open_and_insert_3000_test() ->
  test_cleanup(),
  Tree = lists:foldl(fun(N, Tree) ->
      update(lists:concat(["key", N]), lists:concat(["value", N]), Tree)
    end, open("/Users/cliff/data/dmerkle", 256), lists:seq(1,3000)),
  true = lists:all(fun(N) -> 
      Hash = hash(lists:concat(["value", N])),
      Result = Hash == find(lists:concat(["key", N]), Tree),
      if
        Result -> Result;
        true -> 
          error_logger:info_msg("could not get ~p was ~p~n", [N, find(lists:concat(["key", N]), Tree)]),
          Result
      end
    end, lists:seq(1, 3000)),
  close(Tree).
   
insert_500_both_ways_test() ->
  test_cleanup(),
  TreeA = lists:foldl(fun(N, Tree) ->
      update(lists:concat(["key", N]), lists:concat(["value", N]), Tree)
    end, open("/Users/cliff/data/dmerkle", 256), lists:seq(1,500)),
  TreeB = lists:foldl(fun(N, Tree) ->
      update(lists:concat(["key", N]), lists:concat(["value", N]), Tree)
    end, open("/Users/cliff/data/dmerkle1", 256), lists:reverse(lists:seq(1,500))),
  % error_logger:info_msg("rootA ~p~nrootB ~p~n", [TreeA#dmerkle.root, TreeB#dmerkle.root]),
  true = equals(TreeA, TreeB).
  
insert_realistic_scenario_equality_test() ->
  test_cleanup(),
  TreeA = lists:foldl(fun(N, Tree) ->
      update(lists:concat(["key", N]), lists:concat(["value", N]), Tree)
    end, open("/Users/cliff/data/dmerkle", 256), lists:seq(1,500)),
  TreeB = lists:foldl(fun(N, Tree) ->
      update(lists:concat(["key", N]), lists:concat(["value", N]), Tree)
    end, open("/Users/cliff/data/dmerkle1", 256), lists:seq(1,505)),
  error_logger:info_msg("rootA ~p~nrootB ~p~n", [TreeA#dmerkle.root, TreeB#dmerkle.root]),
  timer:sleep(100),
  false = equals(TreeA, TreeB).

insert_realistic_scenario_diff_test() ->
  test_cleanup(),
  TreeA = lists:foldl(fun(N, Tree) ->
      update(lists:concat(["key", N]), lists:concat(["value", N]), Tree)
    end, open("/Users/cliff/data/dmerkle", 256), lists:seq(1,495)),
  TreeB = lists:foldl(fun(N, Tree) ->
      update(lists:concat(["key", N]), lists:concat(["value", N]), Tree)
    end, open("/Users/cliff/data/dmerkle1", 256), lists:seq(1,500)),
  error_logger:info_msg("rootA ~p~nrootB ~p~n", [TreeA#dmerkle.root, TreeB#dmerkle.root]),
  Diff = key_diff(TreeA, TreeB),
  timer:sleep(100),
  Keys = lists:map(fun(N) -> 
      {lists:concat(["key", N]), hash(lists:concat(["value", N]))}
    end, lists:seq(496, 500)),
  error_logger:info_msg("realistic diff: ~p~n", [Diff]),
  Keys = Diff.
  
insert_500_both_ways_diff_test() ->
  test_cleanup(),
  TreeA = lists:foldl(fun(N, Tree) ->
      update(lists:concat(["key", N]), lists:concat(["value", N]), Tree)
    end, open("/Users/cliff/data/dmerkle", 256), lists:seq(1,500)),
  TreeB = lists:foldl(fun(N, Tree) ->
      update(lists:concat(["key", N]), lists:concat(["value", N]), Tree)
    end, open("/Users/cliff/data/dmerkle1", 256), lists:reverse(lists:seq(1,500))),
  Diff = key_diff(TreeA, TreeB),
  error_logger:info_msg("both ways diff: ~p~n", [Diff]),
  [] = Diff.

% swap_tree_test() ->
%   test_cleanup(),
%   TreeA = lists:foldl(fun(N, Tree) ->
%       update(lists:concat(["key", N]), lists:concat(["value", N]), Tree)
%     end, open("/Users/cliff/data/dmerkle", 256), lists:seq(1,500)),
%   TreeB = lists:foldl(fun(N, Tree) ->
%       update(lists:concat(["key", N]), lists:concat(["value", N]), Tree)
%     end, open("/Users/cliff/data/dmerkle1", 256), lists:reverse(lists:seq(1,250))),
%   NewTree = swap_tree(TreeA, TreeB),
%   SameTree = open("/Users/cliff/data/dmerkle", 256),
%   error_logger:info_msg("trees: ~p ~p~n", [NewTree, SameTree]),
%   timer:sleep(100),
%   [] = key_diff(NewTree, SameTree).
%   
% leaves_test() ->
%   test_cleanup(),
%   Tree = lists:foldl(fun(N, Tree) ->
%       update(lists:concat(["key", N]), lists:concat(["value", N]), Tree)
%     end, open("/Users/cliff/data/dmerkle", 256), lists:seq(1,500)),
%   500 = length(leaves(Tree)).
  % 
  % empty_diff_test() ->
  %   test_cleanup(),
  %   TreeA = lists:foldl(fun(N, Tree) ->
  %       update(lists:concat(["key", N]), lists:concat(["value", N]), Tree)
  %     end, open("/Users/cliff/data/dmerkle", 256), lists:seq(1,500)),
  %   TreeB = open("/Users/cliff/data/dmerkle1", 256),
  %   500 = length(key_diff(TreeA, TreeB)).
  
open_and_insert_n(N) ->
  test_cleanup(),
  Tree = lists:foldl(fun(N, Tree) ->
      update(lists:concat(["key", N]), lists:concat(["value", N]), Tree)
    end, open("/Users/cliff/data/dmerkle", 256), lists:seq(1,N)),
  true = lists:all(fun(N) -> 
      Hash = hash(lists:concat(["value", N])),
      Result = Hash == find(lists:concat(["key", N]), Tree),
      if
        Result -> Result;
        true -> 
          error_logger:info_msg("could not get ~p was ~p~n", [N, find(lists:concat(["key", N]), Tree)]),
          timer:sleep(1000),
          Result
      end
    end, lists:seq(1, N)),
  timer:sleep(100),
  close(Tree).

stress() ->
  test_cleanup(),
  process_flag(trap_exit, true),
  spawn_link(
    fun() -> lists:foldl(fun(N, Tree) ->
        update(lists:concat(["key", N]), lists:concat(["value", N]), Tree)
      end, open("/Users/cliff/data/dmerkle", 4096), lists:seq(1,100000)) 
    end),
  receive _ -> timer:sleep(1) end.