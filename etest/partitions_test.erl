-include_lib("eunit.hrl").

node_hash_test() ->
  ?assertEqual(22, node_hash(a, [a, b, c], 64)),
  ?assertEqual(43, node_hash(b, [a, b, c], 64)),
  ?assertEqual(64, node_hash(c, [a, b, c], 64)).

rebalance_2_test() ->
  Partitions = create_partitions([{a, 64}]),
  Parts1 = map_partitions(Partitions, [a,b]),
  Sizes = sizes([a, b], Parts1),
  timer:sleep(100),
  {value, {a, 32}} = lists:keysearch(a, 1, Sizes),
  {value, {b, 32}} = lists:keysearch(b, 1, Sizes).
  
rebalance_3_test() ->
  Partitions = create_partitions([{a, 32}, {b, 32}]),
  Parts1 = map_partitions(Partitions, [a, b, c]),
  Sizes = sizes([a, b, c], Parts1),
  ?assertEqual({value, {a, 22}}, lists:keysearch(a, 1, Sizes)),
  ?assertEqual({value, {b, 21}}, lists:keysearch(b, 1, Sizes)),
  ?assertEqual({value, {c, 21}}, lists:keysearch(c, 1, Sizes)).

minimal_rebalance_3_test() ->
  Partitions = create_partitions([{a,32},{c,32}]),
  Parts1 = map_partitions(Partitions, [a, b, c]),
  Diff = diff(Partitions, Parts1),
  ?assertEqual(21, length(Diff)).

rebalance_4_test() ->
  Partitions = create_partitions([{a, 22}, {b, 21}, {c, 21}]),
  Parts1 = map_partitions(Partitions, [a, b, c, d]),
  Sizes = sizes([a, b, c, d], Parts1),
  {value, {a, 16}} = lists:keysearch(a, 1, Sizes),
  {value, {b, 16}} = lists:keysearch(b, 1, Sizes),
  {value, {c, 16}} = lists:keysearch(c, 1, Sizes),
  {value, {d, 16}} = lists:keysearch(d, 1, Sizes).

rebalance_rejoin_test() ->
  Partitions = create_partitions([{a, 13}, {b, 13}, {c, 13}, {d, 13}, {e, 12}]),
  Parts1 = map_partitions(Partitions, [a, b, c, d, e]),
  Sizes = sizes([a, b, c, d, e, f], Parts1),
  ?assertEqual({value, {a, 13}}, lists:keysearch(a, 1, Sizes)).

create_partitions(Sizes) ->
  {PartLists, _} = lists:foldl(fun({Node, Size}, {PartLists, Count}) ->
      NewPart = lists:map(fun(Part) -> 
          {Node, Part}
        end, lists:seq(Count, Count+Size-1)),
      {PartLists ++ NewPart, Count+Size}
    end, {[], 1}, Sizes),
  lists:keysort(2, PartLists).