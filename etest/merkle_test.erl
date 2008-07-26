-include_lib("include/eunit/eunit.hrl").

create_test() ->
  Root = create(0, 2 bsl 31),
  Node = Root#root.node,
  (2 bsl 30) = Node#node.middle,
  empty = Node#node.hash.
  
simple_update_test() ->
  Root = create(0, 2 bsl 31),
  NewRoot = update("key1", "value1", Root),
  1 = leaf_size(NewRoot).
  
second_update_test() ->
  Root = create(0, 2 bsl 31),
  NewRoot = update("key2", "value2", update("key1", "value1", Root)),
  2 = leaf_size(NewRoot).
  
third_update_test() ->
  Root = create(0, 2 bsl 31),
  NewRoot = update("key3", "value3", update("key2", "value2", update("key1", "value1", Root))),
  3 = leaf_size(NewRoot).
  
update_replace_value_test() ->
  Root = create(0, 2 bsl 31),
  NewRoot = update("key3", "value3", update("key2", "value2", update("key1", "value1", Root))),
  SecondRoot = update("key1", "wholenewthing", NewRoot),
  true = hash(NewRoot) =/= hash(SecondRoot),
  3 = leaf_size(SecondRoot).
  
simple_key_diff_test() ->
  Root = update("key1", "value1", create(0, 2 bsl 31)),
  NewRoot = update("key1", "value2", Root),
  ["key1"] = key_diff(Root, NewRoot).
  
multiple_key_diff_test() ->
  Root = update("key3", "value3", update("key2", "value2", update("key1", "value1", create(0, 2 bsl 31)))),
  LeftRoot = update("key3", "newvalue", Root),
  RightRoot = update("key2", "newvalue", Root),
  ["key2", "key3"] = lists:sort(key_diff(LeftRoot, RightRoot)).
  
totally_boned_key_diff_test() ->
  Left = update("key3", "value3", update("key2", "value2", update("key1", "value1", create(0, 2 bsl 31)))),
  Right = update("key6", "value6", update("key5", "value5", update("key4", "value4", create(0, 2 bsl 31)))),
  List = lists:sort(key_diff(Left, Right)),
  timer:sleep(100),
  ["key1", "key2", "key3", "key4", "key5", "key6"] = List.
  
delete_test() ->
  Root = create(0, 2 bsl 31),
  NewRoot = delete("key2", update("key3", "value3", update("key2", "value2", update("key1", "value1", Root)))),
  2 = leaf_size(NewRoot).
  
delete_second_test() ->
  Root = create(0, 2 bsl 31),
  NewRoot = delete("key3", delete("key2", update("key3", "value3", update("key2", "value2", update("key1", "value1", Root))))),
  1 = leaf_size(NewRoot).
  
delete_third_test() ->
  Root = create(0, 2 bsl 31),
  Blah = update(two, value2, update(key, value, Root)),
  error_logger:info_msg("blah ~p~n",[Blah]),
  NewRoot = delete(key, Blah),
  error_logger:info_msg("newroot ~p~n",[NewRoot]),
  timer:sleep(100),
  1 = leaf_size(NewRoot).