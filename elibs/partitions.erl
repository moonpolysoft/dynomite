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
-export([rebalance_partitions/3, merge_partitions/4]).

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

rebalance_partitions(NewNode, Nodes, Partitions) ->
  Nodes1 = lists:filter(fun(E) -> E /= NewNode end, Nodes),
  Sizes = sizes(Nodes1, Partitions),
  TargetSize = length(Partitions) div (length(Nodes1) + 1),
  int_rebalance(NewNode, TargetSize, Sizes, Partitions, []).

%%====================================================================
%% Internal functions
%%====================================================================

sizes(Nodes, Partitions) ->
  lists:reverse(lists:keysort(2, 
    lists:map(fun(Node) ->
      Count = lists:foldl(fun
          ({Matched,_}, Acc) when Matched == Node -> Acc+1;
          (_, Acc) -> Acc
        end, 0, Partitions),
      {Node, Count}
    end, Nodes))).

int_rebalance(Node, TargetSize, Sizes, Partitions, Taken) when TargetSize =< length(Sizes) ->
  io:format("int_rebalance: ~p~n", [{Node, TargetSize, Sizes, Partitions, Taken}]),
  {Partitions1, Taken1} = take_n(Node, TargetSize, Sizes, Partitions, Taken),
  % error_logger:info_msg("end partitions, taken = {~w, ~w}~n", [Partitions1, Taken1]),
  lists:keysort(2, Partitions1 ++ Taken1);
  
int_rebalance(Node, TargetSize, Sizes, Partitions, Taken) ->
  {Partitions1, Taken1} = take_n(Node, length(Sizes), Sizes, Partitions, Taken),
  % error_logger:info_msg("partitions, taken = {~w, ~w}~n", [Partitions1, Taken1]),
  int_rebalance(Node, TargetSize - length(Sizes), Sizes, Partitions1, Taken1).
  
take_n(_, 0, _, Partitions, Taken) ->
  {Partitions, Taken};
  
take_n(Node, N, [{TakeFrom,Size}|Sizes], Partitions, Taken) ->
  case lists:keytake(TakeFrom, 1, Partitions) of
    {value, {TakeFrom, Part}, Partitions1} -> take_n(Node, N-1, Sizes, Partitions1, [{Node,Part}|Taken]);
    false -> take_n(Node, N, Sizes, Partitions, Taken)
  end.
  
merge_partitions(PartA, PartB, N, Nodes) ->
  merge_partitions(PartA, PartB, [], N, Nodes).

merge_partitions([], [], Result, _, _) -> lists:keysort(2, Result);
merge_partitions(A, [], Result, _, _) -> lists:keysort(2, A ++ Result);
merge_partitions([], B, Result, _, _) -> lists:keysort(2, B ++ Result);

merge_partitions([{NodeA,Number}|PartA], [{NodeB,Number}|PartB], Result, N, Nodes) ->
  if
    NodeA == NodeB -> 
      merge_partitions(PartA, PartB, [{NodeA,Number}|Result], N, Nodes);
    true ->
      case within(N, NodeA, NodeB, Nodes) of
        {true, First} -> merge_partitions(PartA, PartB, [{First,Number}|Result], N, Nodes);
        % bah, maybe we should just fucking pick one
        _ -> merge_partitions(PartA, PartB, [{NodeA,Number}|Result], N, Nodes)
      end
  end;
  
merge_partitions([{NodeA,PA}|PartA], [{NodeB,PB}|PartB], Result, N, Nodes) ->
  error_logger:info_msg("merge_partitions error: {~p,~p}, {~p,~p}~n", [NodeA,PA,NodeB,PB]),
  throw("FUCK").

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
