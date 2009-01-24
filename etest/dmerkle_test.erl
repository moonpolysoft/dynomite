-include_lib("eunit/include/eunit.hrl").

test_cleanup() ->
  file:delete(data_file()),
  file:delete(data_file(1)).

      
open_and_close_test() ->
  test_cleanup(),
  {ok, Pid} = open(data_file(), 256),
  Merkle = get_state(Pid),
  Root = Merkle#dmerkle.root,
  error_logger:info_msg("root ~p~n", [Root]),
  ?assertEqual(?HEADER_SIZE, Root#leaf.offset),
  ?assertEqual(0, Root#leaf.m),
  close(Pid).
  
open_and_insert_one_test() ->
  test_cleanup(),
  {ok, Pid} = open(data_file(), 256),
  update("mykey", <<"myvalue">>, Pid),
  Tree = get_state(Pid),
  Root = Tree#dmerkle.root,
  error_logger:info_msg("root w/ one ~p merkle~p~n", [Root, Tree]),
  ?assertEqual(1, Root#leaf.m),
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

open_and_insert_20_test() ->
  open_and_insert_n(20).

open_and_insert_260_test() ->
  open_and_insert_n(260).
  
open_and_insert_1000_test() ->
  open_and_insert_n(1000).
  
open_and_insert_3000_test() ->
  open_and_insert_n(3000).
   
insert_500_both_ways_test() ->
  test_cleanup(),
  {ok, Pid} = open(data_file(), 256),
  lists:foldl(fun(N, Tree) ->
      update(lists:concat(["key", N]), lists:concat(["value", N]), Tree)
    end, Pid, lists:seq(1,500)),
  {ok, Pid2} = open(data_file(1), 256),
  lists:foldl(fun(N, Tree) ->
      update(lists:concat(["key", N]), lists:concat(["value", N]), Tree)
    end, Pid2, lists:reverse(lists:seq(1,500))),
  TreeA = get_state(Pid),
  TreeB = get_state(Pid2),
  ?infoFmt("leaves A: ~p~n", [leaves(Pid)]),
  ?infoFmt("leaves B: ~p~n", [leaves(Pid2)]),
  LeafHashA = lists:foldl(fun({_,Hash}, Sum) ->
      (Hash + Sum) rem (2 bsl 31)
    end, 0, leaves(Pid)),
  LeafHashB = lists:foldl(fun({_,Hash}, Sum) ->
      (Hash + Sum) rem (2 bsl 31)
    end, 0, leaves(Pid2)),
  ?assertEqual(leaves(Pid), leaves(Pid2)),
  ?assertEqual(true, equals(Pid, Pid2)),
  close(Pid),
  close(Pid2).
  
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
  ?assertEqual(Keys, Diff),
  close(Pid),
  close(Pid2).

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
  % error_logger:info_msg("both ways diff: ~p~n", [key_diff(TreeA, TreeB)]),
  ?assertEqual([], key_diff(TreeA, TreeB)),
  close(PidA),
  close(PidB).

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

%% swapping trees may not be something we want to support nomo

% swap_tree_test() ->
%   test_cleanup(),
%   {ok, PidA} = open(data_file(), 256),
%   TreeA = lists:foldl(fun(N, Tree) ->
%       update(lists:concat(["key", N]), lists:concat(["value", N]), Tree)
%     end, PidA, lists:seq(1,250)),
%   {ok, PidB} = open(data_file(1), 256),
%   TreeB = lists:foldl(fun(N, Tree) ->
%       update(lists:concat(["key", N]), lists:concat(["value", N]), Tree)
%     end, PidB, lists:reverse(lists:seq(1,500))),
%   {ok, NewTree} = swap_tree(TreeA, TreeB),
%   true = lists:all(fun(N) -> 
%       Hash = hash(lists:concat(["value", N])),
%       Result = Hash == find(lists:concat(["key", N]), NewTree),
%       if
%         Result -> Result;
%         true -> 
%           error_logger:info_msg("could not get ~p was ~p~n", [N, find(lists:concat(["key", N]), NewTree)]),
%           Result
%       end
%     end, lists:seq(1, 500)).
%   
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
  Tree = get_state(Pid),
  Root = Tree#dmerkle.root,
  error_logger:info_msg("Root ~p~n", [Root]),
  ?assertEqual(0, Root#leaf.m),
  close(Pid).
  
full_deletion_with_single_split_test() ->
  test_cleanup(),
  {ok, Pid} = open(data_file(), 256),
  lists:foldl(fun(N, Tree) ->
      update(lists:concat(["key", N]), lists:concat(["value", N]), Tree)
    end, Pid, lists:seq(1,20)),
  lists:foreach(fun(N) ->
      Key = lists:concat(["key", N]),
      delete(Key, Pid)
    end, lists:seq(1,20)),
  Tree = get_state(Pid),
  Root = Tree#dmerkle.root,
  ?assertMatch(#leaf{}, Root),
  ?assertEqual(0, Root#leaf.m),
  close(Pid).
  
compare_trees_with_delete_test() ->
  test_cleanup(),
  {ok, PidA} = open(data_file(), 256),
  {ok, PidB} = open(data_file(1), 256),
  lists:foldl(fun(N, Tree) ->
      update(lists:concat(["key", N]), lists:concat(["value", N]), Tree)
    end, PidA, lists:seq(1,20)),
  lists:foldl(fun(N, Tree) ->
      update(lists:concat(["key", N]), lists:concat(["value", N]), Tree)
    end, PidB, lists:seq(1,20)),
  delete("key1", PidB),
  ?assertEqual(["key1"], key_diff(PidA, PidB)),
  close(PidA),
  close(PidB).
  
full_deletion_with_multiple_split_test_() ->
  {timeout, 120, [{?LINE, fun() ->
    test_cleanup(),
    {ok, Pid} = open(data_file(), 256),
    lists:foldl(fun(N, Tree) ->
        update(lists:concat(["key", N]), lists:concat(["value", N]), Tree)
      end, Pid, lists:seq(1,300)),
    lists:foldl(fun(N, Tree) ->
        Key = lists:concat(["key", N]),
        ?infoFmt("deleting ~p~n", [Key]),
        delete(Key, Tree),
        ?assertEqual(300-N, length(leaves(Tree))),
        Tree
      end, Pid, lists:seq(1,300)),
    Tree = get_state(Pid),
    Root = Tree#dmerkle.root,
    ?infoFmt("root: ~p~n", [Tree#dmerkle.root]),
    ?assertMatch(#leaf{}, Root),
    ?assertEqual(0, Root#leaf.m),
    close(Pid)
  end}]}.
  
partial_deletion_with_multiple_split_test_() ->
  {timeout, 120, [{?LINE, fun() ->
    test_cleanup(),
    {ok, Pid1} = open(data_file(), 256),
    {ok, Pid2} = open(data_file(1), 256),
    Keys = lists:map(fun(I) ->
        lib_misc:rand_str(10)
      end, lists:seq(1,300)),
    lists:foreach(fun(Key) ->
        update(Key, "vallllllueeee" ++ Key, Pid1),
        update(Key, "vallllllueeee" ++ Key, Pid2)
      end, Keys),
    lists:foreach(fun(Key) ->
        delete(Key, Pid2)
      end, lists:sublist(Keys, 50)),
    ?assertEqual(lists:sort(lists:sublist(Keys, 50)), key_diff(Pid1, Pid2)),
    close(Pid1),
    close(Pid2)
  end}]}.

partial_deletion_and_rebuild_test() ->
  test_cleanup(),
  {ok, Pid} = open(data_file(), 256),
  Keys = lists:map(fun(I) ->
      lib_misc:rand_str(10)
    end, lists:seq(1,300)),
  lists:foreach(fun(Key) ->
      update(Key, "valuuueeeee" ++ Key, Pid)
    end, Keys),
  IdxSize = filelib:file_size(data_file()),
  lists:foreach(fun(Key) ->
      delete(Key, Pid)
    end, lists:sublist(Keys, 100)),
  lists:foreach(fun(Key) ->
      update(Key, "valuuueeeee" ++ Key, Pid)
    end, lists:sublist(Keys, 100)),
  ?assertEqual(IdxSize, filelib:file_size(data_file())),
  close(Pid).

open_and_insert_n(N) ->
  test_cleanup(),
  {ok, Pid} = open(data_file(), 256),
  lists:foreach(fun(N) ->
      Key = lists:concat(["key", N]),
      Value = lists:concat(["value", N]),
      update(Key, Value, Pid)
      % ?infoFmt("leaves ~p~n", [leaves(Pid)])
    end, lists:seq(1,N)),
  ?assertEqual(true, lists:all(fun(N) -> 
      Hash = hash(lists:concat(["value", N])),
      Result = Hash == find(lists:concat(["key", N]), Pid),
      if
        Result -> Result;
        true -> 
          error_logger:info_msg("could not get ~p was ~p~n", [N, find(lists:concat(["key", N]), Pid)]),
          Result
      end
    end, lists:seq(1, N))),
  close(Pid).

stress() ->
  test_cleanup(),
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
