-include_lib("eunit.hrl").

% -export([stress/0]).

test_cleanup() ->
  file:delete(data_file() ++ ".idx"),
  file:delete(data_file() ++ ".keys"),
  file:delete(data_file() ++ "1.idx"),
  file:delete(data_file() ++ "1.keys").

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
  {ok, Pid} = open(data_file(), 256),
  Merkle = get_tree(Pid),
  Root = Merkle#dmerkle.root,
  error_logger:info_msg("root ~p~n", [Root]),
  ?assertEqual(?HEADER_SIZE, Root#leaf.offset),
  ?assertEqual(0, Root#leaf.m),
  close(Pid).
  
open_and_insert_one_test() ->
  test_cleanup(),
  {ok, Pid} = open(data_file(), 256),
  update("mykey", <<"myvalue">>, Pid),
  Tree = get_tree(Pid),
  Root = Tree#dmerkle.root,
  error_logger:info_msg("root w/ one ~p merkle~p~n", [Root, Tree]),
  1 = Root#leaf.m,
  Hash = hash(<<"myvalue">>),
  Hash = find("mykey", Pid),
  close(Pid).
  
open_and_reopen_test() ->
  test_cleanup(),
  {ok, Pid} = open(data_file(), 256),
  update("mykey", <<"myvalue">>, Pid),
  close(Pid),
  {ok, NewPid} = open(data_file(), 256),
  Hash = hash(<<"myvalue">>),
  Hash = find("mykey", NewPid),
  close(NewPid).

open_and_insert_260_test() ->
  open_and_insert_n(260).
  
open_and_insert_1000_test() ->
  open_and_insert_n(1000).
  
open_and_insert_3000_test() ->
  open_and_insert_n(3000).
   
insert_500_both_ways_test() ->
  test_cleanup(),
  {ok, Pid} = open(data_file(), 256),
  TreeA = lists:foldl(fun(N, Tree) ->
      update(lists:concat(["key", N]), lists:concat(["value", N]), Tree)
    end, Pid, lists:seq(1,500)),
  {ok, Pid2} = open(data_file(1), 256),
  TreeB = lists:foldl(fun(N, Tree) ->
      update(lists:concat(["key", N]), lists:concat(["value", N]), Tree)
    end, Pid2, lists:reverse(lists:seq(1,500))),
  % error_logger:info_msg("rootA ~p~nrootB ~p~n", [TreeA#dmerkle.root, TreeB#dmerkle.root]),
  true = equals(TreeA, TreeB).
  
insert_realistic_scenario_equality_test() ->
  test_cleanup(),
  {ok, Pid} = open(data_file(), 256),
  TreeA = lists:foldl(fun(N, Tree) ->
      update(lists:concat(["key", N]), lists:concat(["value", N]), Tree)
    end, Pid, lists:seq(1,500)),
  {ok, Pid2} = open(data_file(1), 256),
  TreeB = lists:foldl(fun(N, Tree) ->
      update(lists:concat(["key", N]), lists:concat(["value", N]), Tree)
    end, Pid2, lists:seq(1,505)),
  false = equals(TreeA, TreeB).

insert_realistic_scenario_diff_test() ->
  test_cleanup(),
  {ok, Pid} = open(data_file(), 256),
  TreeA = lists:foldl(fun(N, Tree) ->
      update(lists:concat(["key", N]), lists:concat(["value", N]), Tree)
    end, Pid, lists:seq(1,495)),
  {ok, Pid2} = open(data_file(1), 256),
  TreeB = lists:foldl(fun(N, Tree) ->
      update(lists:concat(["key", N]), lists:concat(["value", N]), Tree)
    end, Pid2, lists:seq(1,500)),
  Diff = key_diff(TreeA, TreeB),
  Keys = lists:map(fun(N) -> lists:concat(["key", N]) end, lists:seq(496, 500)),
  error_logger:info_msg("realistic diff: ~p~n", [Diff]),
  Keys = Diff.

insert_500_both_ways_diff_test() ->
  test_cleanup(),
  {ok, PidA} = open(data_file(), 256),
  TreeA = lists:foldl(fun(N, Tree) ->
      update(lists:concat(["key", N]), lists:concat(["value", N]), Tree)
    end, PidA, lists:seq(1,500)),
  {ok, PidB} = open(data_file(1), 256),
  TreeB = lists:foldl(fun(N, Tree) ->
      update(lists:concat(["key", N]), lists:concat(["value", N]), Tree)
    end, PidB, lists:reverse(lists:seq(1,500))),
  Diff = key_diff(TreeA, TreeB),
  error_logger:info_msg("both ways diff: ~p~n", [Diff]),
  [] = Diff.

insert_overwrite_test() ->
  test_cleanup(),
  {ok, PidA} = open(data_file(), 256),
  TreeA = lists:foldl(fun(N, Tree) ->
      update(lists:concat(["key", N]), lists:concat(["value", N]), Tree)
    end, PidA, lists:seq(1,500)),
  {ok, PidB} = open(data_file(1), 256),
  TreeB = lists:foldl(fun(N, Tree) ->
      update(lists:concat(["key", N]), lists:concat(["different", N]), Tree)
    end, lists:foldl(fun(N, Tree) ->
        update(lists:concat(["key", N]), lists:concat(["value", N]), Tree)
      end, PidB, lists:seq(1,500)), lists:seq(1, 500)),
  Diff = key_diff(TreeA, TreeB),
  500 = length(Diff),
  500 = length(leaves(TreeB)).
  
insert_overwrite2_test() ->
  test_cleanup(),
  {ok, PidA} = open(data_file(), 256),
  TreeA = lists:foldl(fun(N, Tree) ->
      update(lists:concat(["key", N]), lists:concat(["value", N]), Tree)
    end, PidA, lists:seq(1,3000)),
  {ok, PidB} = open(data_file(1), 256),
  TreeB = lists:foldl(fun(N, Tree) ->
      update(lists:concat(["key"]), lists:concat(["value", N]), Tree)
    end, lists:foldl(fun(N, Tree) ->
        update(lists:concat(["key"]), lists:concat(["value", N]), Tree)
      end, PidB, lists:seq(1,3000)), lists:seq(1, 3000)),
  % Diff = key_diff(TreeA, TreeB),
  % [] = Diff,
  1 = length(leaves(TreeB)).

swap_tree_test() ->
  test_cleanup(),
  {ok, PidA} = open(data_file(), 256),
  TreeA = lists:foldl(fun(N, Tree) ->
      update(lists:concat(["key", N]), lists:concat(["value", N]), Tree)
    end, PidA, lists:seq(1,250)),
  {ok, PidB} = open(data_file(1), 256),
  TreeB = lists:foldl(fun(N, Tree) ->
      update(lists:concat(["key", N]), lists:concat(["value", N]), Tree)
    end, PidB, lists:reverse(lists:seq(1,500))),
  {ok, NewTree} = swap_tree(TreeA, TreeB),
  true = lists:all(fun(N) -> 
      Hash = hash(lists:concat(["value", N])),
      Result = Hash == find(lists:concat(["key", N]), NewTree),
      if
        Result -> Result;
        true -> 
          error_logger:info_msg("could not get ~p was ~p~n", [N, find(lists:concat(["key", N]), NewTree)]),
          Result
      end
    end, lists:seq(1, 500)).
  
leaves_test() ->
  test_cleanup(),
  {ok, PidA} = open(data_file(), 256),
  Tree = lists:foldl(fun(N, Tree) ->
      update(lists:concat(["key", N]), lists:concat(["value", N]), Tree)
    end, PidA, lists:seq(1,500)),
  500 = length(leaves(Tree)).
  
empty_diff_test() ->
  test_cleanup(),
  {ok, PidA} = open(data_file(), 256),
  TreeA = lists:foldl(fun(N, Tree) ->
      update(lists:concat(["key", N]), lists:concat(["value", N]), Tree)
    end, PidA, lists:seq(1,500)),
  {ok, TreeB} = open(data_file(1), 256),
  500 = length(key_diff(TreeA, TreeB)).
  
live_test_() ->
  {timeout, 120, [{?LINE, fun() ->
    {ok, TreeA} = open(data_file(410), 4096),
    {ok, TreeB} = open(data_file(42), 4096),
    KeyDiff = key_diff(TreeA, TreeB),
    error_logger:info_msg("key_diff: ~p~n", [KeyDiff]),
    LeavesA = leaves(TreeA),
    LeavesB = leaves(TreeB),
    LeafDiff = LeavesA -- LeavesB,
    error_logger:info_msg("leaf_diff: ~p~n", [LeafDiff]),
    timer:sleep(100),
    KeyDiff = LeafDiff
  end}]}.
  
simple_deletion_test() ->
  test_cleanup(),
  {ok, Pid} = open(data_file(), 256),
  update("key", "value", Pid),
  delete("key", Pid),
  Tree = get_tree(Pid),
  Root = Tree#dmerkle.root,
  error_logger:info_msg("Root ~p~n", [Root]),
  ?assertEqual(0, Root#leaf.m),
  close(Pid).
  
open_and_insert_n(N) ->
  test_cleanup(),
  {ok, Pid} = open(data_file(), 256),
  Tree = lists:foldl(fun(N, Tree) ->
      update(lists:concat(["key", N]), lists:concat(["value", N]), Tree)
    end, Pid, lists:seq(1,N)),
  true = lists:all(fun(N) -> 
      Hash = hash(lists:concat(["value", N])),
      Result = Hash == find(lists:concat(["key", N]), Tree),
      if
        Result -> Result;
        true -> 
          error_logger:info_msg("could not get ~p was ~p~n", [N, find(lists:concat(["key", N]), Tree)]),
          Result
      end
    end, lists:seq(1, N)),
  close(Tree).

stress() ->
  test_cleanup(),
  process_flag(trap_exit, true),
  spawn_link(
    fun() -> lists:foldl(fun(N, Tree) ->
        update(lists:concat(["key", N]), lists:concat(["value", N]), Tree)
      end, open(data_file(), 4096), lists:seq(1,100000)) 
    end),
  receive _ -> timer:sleep(1) end.

priv_dir() ->
    Dir = filename:join(t:config(priv_dir), "data"),
    filelib:ensure_dir(filename:join(Dir, "dmerkle")),
    Dir.

data_file() ->
    filename:join(priv_dir(), "dmerkle").
   
data_file(N) ->
    filename:join(priv_dir(), "dmerkle" ++ integer_to_list(N)).
