-include_lib("eunit.hrl").

rebalance_2_test() ->
  Partitions = create_partitions([{a, 64}]),
  Parts1 = rebalance_partitions(b, [a], Partitions),
  Sizes = sizes([a, b], Parts1),
  timer:sleep(100),
  {value, {a, 32}} = lists:keysearch(a, 1, Sizes),
  {value, {b, 32}} = lists:keysearch(b, 1, Sizes).
  
rebalance_3_test() ->
  Partitions = create_partitions([{a, 32}, {b, 32}]),
  Parts1 = rebalance_partitions(c, [a, b], Partitions),
  Sizes = sizes([a, b, c], Parts1),
  timer:sleep(100),
  {value, {a, 22}} = lists:keysearch(a, 1, Sizes),
  {value, {b, 21}} = lists:keysearch(b, 1, Sizes),
  {value, {c, 21}} = lists:keysearch(c, 1, Sizes).
  
rebalance_4_test() ->
  Partitions = create_partitions([{a, 22}, {b, 21}, {c, 21}]),
  Parts1 = rebalance_partitions(d, [a, b, c], Partitions),
  Sizes = sizes([a, b, c, d], Parts1),
  {value, {a, 16}} = lists:keysearch(a, 1, Sizes),
  {value, {b, 16}} = lists:keysearch(b, 1, Sizes),
  {value, {c, 16}} = lists:keysearch(c, 1, Sizes),
  {value, {d, 16}} = lists:keysearch(d, 1, Sizes).
  
rebalance_5_test() ->
  Partitions = create_partitions([{a, 16}, {b, 16}, {c, 16}, {d, 16}]),
  Parts1 = rebalance_partitions(e, [a, b, c, d], Partitions),
  Sizes = sizes([a, b, c, d, e], Parts1),
  {value, {a, 13}} = lists:keysearch(a, 1, Sizes),
  {value, {b, 13}} = lists:keysearch(b, 1, Sizes),
  {value, {c, 13}} = lists:keysearch(c, 1, Sizes),
  {value, {d, 13}} = lists:keysearch(d, 1, Sizes),
  {value, {e, 12}} = lists:keysearch(e, 1, Sizes).
  
rebalance_6_test() ->
  Partitions = create_partitions([{a, 13}, {b, 13}, {c, 13}, {d, 13}, {e, 12}]),
  Parts1 = rebalance_partitions(f, [a, b, c, d, e], Partitions),
  Sizes = sizes([a, b, c, d, e, f], Parts1),
  {value, {a, 11}} = lists:keysearch(a, 1, Sizes),
  {value, {b, 11}} = lists:keysearch(b, 1, Sizes),
  {value, {c, 11}} = lists:keysearch(c, 1, Sizes),
  {value, {d, 11}} = lists:keysearch(d, 1, Sizes),
  {value, {e, 10}} = lists:keysearch(e, 1, Sizes),
  {value, {f, 10}} = lists:keysearch(f, 1, Sizes).
  
merge_same_partitions_test() ->
  Partitions = create_partitions([{a, 64}]),
  Parts1 = rebalance_partitions(b, [a], Partitions),
  Merged = merge_partitions(Parts1, Parts1, 2, [a, b]),
  Parts1 = Merged.
  
create_partitions(Sizes) ->
  {PartLists, _} = lists:foldl(fun({Node, Size}, {PartLists, Count}) ->
      NewPart = lists:map(fun(Part) -> 
          {Node, Part}
        end, lists:seq(Count, Count+Size-1)),
      {PartLists ++ NewPart, Count+Size+1}
    end, {[], 1}, Sizes),
  lists:keysort(1, PartLists).
  
rejoin_node_test() ->
  Partitions = create_partitions([{a, 32}, {b, 32}]),
  Parts1 = rebalance_partitions(b, [a,b], Partitions),
  error_logger:info_msg("~p ~p ~n", [Partitions, Parts1]),
    timer:sleep(100),
  Partitions = Parts1.