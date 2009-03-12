%%%-------------------------------------------------------------------
%%% File:      partitions.erl
%%% @author    Cliff Moon <cliff@powerset.com> [http://www.powerset.com/]
%%% @copyright 2008 Cliff Moon
%%% @doc  
%%%
%%% @end  
%%%
%%% @since 2008-10-12 by Cliff Moon
%%%-------------------------------------------------------------------
-module(partitions).
-author('cliff@powerset.com').

%% API
-export([partition_range/1, create_partitions/3, map_partitions/2, diff/2, sizes/2]).

-define(power_2(N), (2 bsl (N-1))).

-ifdef(TEST).
-include("etest/partitions_test.erl").
-endif.

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% @spec 
%% @doc
%% @end 
%%--------------------------------------------------------------------

partition_range(Q) -> ?power_2(32-Q).

create_partitions(Q, Node, Nodes) ->
  P = lists:map(fun(P) -> {Node, P} end, lists:seq(1, ?power_2(32), partition_range(Q))),
  map_partitions(P, Nodes).
  
map_partitions(Partitions, Nodes) ->
  {_, Parts} = lists:unzip(Partitions),
  ConsHashMap = hash_map(Nodes),
  % ?debugFmt("conshashmap ~p", [ConsHashMap]),
  do_map(ConsHashMap, Parts).
  
diff(From, To) when length(From) =/= length(To) ->
  throw("Cannot diff partition maps with different length");
  
diff(From, To) ->
  diff(From , To, []).
%%====================================================================
%% Internal functions
%%====================================================================
diff([], [], Results) ->
  lists:reverse(Results);
  
diff([{Node,Part}|PartsA], [{Node,Part}|PartsB], Results) ->
  diff(PartsA, PartsB, Results);
  
diff([{NodeA,Part}|PartsA], [{NodeB,Part}|PartsB], Results) ->
  diff(PartsA, PartsB, [{NodeA,NodeB,Part}|Results]).

hash_map(List) ->
  hash_map(List, []).
  
hash_map([], Acc) -> lists:keysort(1, Acc);

hash_map([Item|List], Acc) ->
  hash_map(List, hash_map(500, Item, [{murmur:hash(Item),Item}|Acc])).
  
hash_map(0, Item, Acc) ->
  Acc;
  
hash_map(N, Item, [{Seed,Item}|Acc]) ->
  hash_map(N-1, Item, [{murmur:hash(Item,Seed),Item},{Seed,Item}|Acc]).

do_map([{Hash,Node}|ConsHashMap], Parts) ->
  do_map({Hash,Node}, [{Hash,Node}|ConsHashMap], Parts, []).
  
do_map({Hash,Node}, [], Parts, Mapped) ->
  lists:keysort(2, lists:map(fun(Part) -> {Node,Part} end, Parts) ++ Mapped);
  
do_map(_, _, [], Mapped) ->
  lists:keysort(2, Mapped);
  
do_map(First, ConsHashMap, [Part|Parts], Mapped) ->
  % ?debugFmt("do_map ~p, ConsHashMap, [~p|~p], ~p", [First, Part, Parts, Mapped]),
  case ConsHashMap of
    [{Hash,Node}|Rest] when Part =< Hash ->
      do_map(First, ConsHashMap, Parts, [{Node,Part}|Mapped]);
    [_|Rest] ->
      do_map(First, Rest, [Part|Parts], Mapped)
  end.

map_partitions([], _, _, Results) ->
  lists:reverse(Results);

map_partitions([{_Old,Part}|Parts], [Hash|Hashes], [Node|Nodes], Results) ->
  if
    Part < Hash -> map_partitions(Parts, [Hash|Hashes], [Node|Nodes], [{Node,Part}|Results]);
    Part == Hash -> map_partitions(Parts, Hashes, Nodes, [{Node,Part}|Results]);
    % can this happen?  hope not.
    true -> map_partitions([{_Old,Part}|Parts], Hashes, Nodes, Results)
  end.

sizes(Nodes, Partitions) ->
  lists:reverse(lists:keysort(2, 
    lists:map(fun(Node) ->
      Count = lists:foldl(fun
          ({Matched,_}, Acc) when Matched == Node -> Acc+1;
          (_, Acc) -> Acc
        end, 0, Partitions),
      {Node, Count}
    end, Nodes))).

within(N, NodeA, NodeB, Nodes) ->
  within(N, NodeA, NodeB, Nodes, nil).

within(_, _, _, [], _) -> false;

within(N, NodeA, NodeB, [Head|Nodes], nil) ->
  case Head of
    NodeA -> within(N-1, NodeB, nil, Nodes, NodeA);
    NodeB -> within(N-1, NodeA, nil, Nodes, NodeB);
    _ -> within(N-1, NodeA, NodeB, Nodes, nil)
  end;

within(0, _, _, _, _) -> false;

within(N, Last, nil, [Head|Nodes], First) ->
  case Head of
    Last -> {true, First};
    _ -> within(N-1, Last, nil, Nodes, First)
  end.
  
node_hash(Name, Nodes, Max) ->
  C = Max / length(Nodes),
  lib_misc:ceiling(C * lib_misc:position(Name, Nodes)).
  