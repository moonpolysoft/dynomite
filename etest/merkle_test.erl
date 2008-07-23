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