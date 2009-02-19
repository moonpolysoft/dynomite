-include_lib("eunit/include/eunit.hrl").

deserialize_node_test() ->
  NodeBin = <<0:8, 2:32, 
    1:32, 2:32, 0:32, 0:32,
    3:32, 4:64, 5:32, 6:64, 7:32, 8:64, 0:32, 0:64, 0:32, 0:64>>,
  Node = deserialize(NodeBin, 4, 20),
  #node{m=2,children=Children,keys=Keys,offset=20} = Node,
  [{3, 4}, {5, 6}, {7, 8}] = Children,
  [1, 2] = Keys.
  
deserialize_leaf_test() ->
  LeafBin = <<1:8, 2:32,
    1:32, 2:64, 3:32,
    4:32, 5:64, 6:32,
    0:352>>,
  Leaf = deserialize(LeafBin, 4, 20),
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
  Node = deserialize(serialize(Node, 81), 4, 0).

leaf_round_trip_test() ->
  Leaf = #leaf{
    m=2,
    values=[{1, 2, 3}, {4, 5, 6}],
    offset=0
  },
  Leaf = deserialize(serialize(Leaf, 81), 4, 0).

pointers_for_blocksize_test() ->
  ?assertEqual(5, ?pointers_from_blocksize(256)),
  ?assertEqual(1, ?pointers_from_blocksize(16)).
  
pointer_for_size_test() ->
  ?assertEqual(1, ?pointer_for_size(14, 4096)).
  
size_for_pointer_test() ->
  ?assertEqual(16, ?size_for_pointer(1)),
  ?assertEqual(256, ?size_for_pointer(5)).
  
open_and_reopen_test() ->
  {ok, Pid} = dmtree:start_link(data_file(), 4096),
  Root = root(Pid),
  State = state(Pid),
  dmtree:stop(Pid),
  {ok, P2} = dmtree:start_link(data_file(), 4096),
  ?assertEqual(Root, root(P2)),
  S2 = state(P2),
  ?assertEqual(State#dmtree.kfpointers, S2#dmtree.kfpointers),
  dmtree:stop(Pid).
  
adjacent_blocks_test() ->
  {ok, Pid} = dmtree:start_link(fixture("dm_adjacentblocks.idx"), 4096),
  dmtree:delete_key(4741, "afknf", Pid),
  dmtree:stop(Pid).
  
fixture_dir() ->
  filename:join(t:config(test_dir), "fixtures").

fixture(Name) -> % need to copy the fixture for repeatability
  file:copy(filename:join(fixture_dir(), Name), data_file(Name)),
  data_file(Name).
  
priv_dir() ->
    Dir = filename:join([t:config(priv_dir), "data", "dmtree"]),
    filelib:ensure_dir(Dir ++ "/"),
    Dir.

data_file(Name) ->
  filename:join(priv_dir(), Name).

data_file() ->
    filename:join(priv_dir(), "dmtree").