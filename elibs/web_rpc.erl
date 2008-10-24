%%%-------------------------------------------------------------------
%%% File:      web_rpc.erl
%%% @author    Cliff Moon <cliff@powerset.com> []
%%% @copyright 2008 Cliff Moon
%%% @doc  
%%%
%%% @end  
%%%
%%% @since 2008-08-14 by Cliff Moon
%%%-------------------------------------------------------------------
-module(web_rpc).
-author('cliff@powerset.com').

%% API
-export([info/1, rates/1, syncs_running/1, diff_size/1]).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% @spec 
%% @doc
%% @end 
%%--------------------------------------------------------------------

info(stats) ->
  {obj, [
    {node,node()}, 
    {running_nodes,lists:sort(nodes([this,visible]))},
    {member_nodes,transform_partitions(lists:keysort(1, membership:partitions()))}
  ]};
  
info(partitions) ->
  lists:map(fun({Node, Part}) -> 
      {obj, [{node,Node},{partition,Part}]}
    end,   lists:keysort(2, membership:partitions()));
    
info(nodes) ->
  membership:nodes().

rates(cluster) ->
  {obj, [
    {get_rate, lists:foldl(fun(Node, Acc) -> Acc + stats_server:rate(Node, get_rate, 1) end, 0, nodes([this,visible]))},
    {put_rate, lists:foldl(fun(Node, Acc) -> Acc + stats_server:rate(Node, put_rate, 1) end, 0, nodes([this,visible]))},
    {in_rate, lists:foldl(fun(Node, Acc) -> Acc + stats_server:rate(Node, in_rate, 1) end, 0, nodes([this,visible]))},
    {out_rate, lists:foldl(fun(Node, Acc) -> Acc + stats_server:rate(Node, out_rate, 1) end, 0, nodes([this,visible]))},
    {connections, lists:foldl(fun(Node, Acc) -> Acc + socket_server:connections(Node) end, 0, nodes([this,visible]))}
  ]};

rates(nodes) ->
  lists:map(fun(Node) -> rates(Node) end, membership:nodes());

rates(Node) ->
  {obj, [
    {name, Node},
    {get_rate, stats_server:rate(Node, get_rate, 1)},
    {put_rate, stats_server:rate(Node, put_rate, 1)},
    {in_rate, stats_server:rate(Node, in_rate, 1)},
    {out_rate, stats_server:rate(Node, out_rate, 1)},
    {connections, socket_server:connections(Node)}
  ]}.
  
syncs_running(cluster) ->
  {Good,_} = rpc:multicall(sync_manager, running, []),
  lists:map(fun({Part, NodeA, NodeB}) -> 
      {obj, [{partition, Part}, {nodes, [NodeA, NodeB]}]}
    end, lists:flatten(Good));
  
syncs_running(Node) ->
  lists:map(fun({Part, NodeA, NodeB}) -> 
      {obj, [{partition, Part}, {nodes, [NodeA, NodeB]}]}
    end, sync_manager:running(Node)).

diff_size(cluster) ->
  {Good,_} = rpc:multicall(sync_manager, diffs, []),
  {obj,
      lists:map(fun({Part, Diffs}) ->
          {integer_to_list(Part), {obj, Diffs}}
        end, lists:flatten(Good))
    }.

%%====================================================================
%% Internal functions
%%====================================================================

transform_partitions(Partitions) ->
  lists:map(fun([Node,Parts]) -> 
      {obj, [
        {name, Node},
        {partitions, Parts},
        {replicas, membership:replica_nodes(Node)}
      ]}
    end, transform_partitions([], Partitions)).

transform_partitions([], [{Node,Part}|Parts]) ->
  transform_partitions([[Node,[Part]]], Parts);
  
transform_partitions(NodeParts, []) ->
  [[Node, lists:sort(Parts)] || [Node, Parts] <- lists:sort(NodeParts)]; 
  
transform_partitions([[Node,NodeParts]|Others], [{Node,Part}|Parts]) ->
  transform_partitions([[Node,[Part|NodeParts]]|Others], Parts);
  
transform_partitions(Others, [{Node,Part}|Parts]) ->
  transform_partitions([[Node,[Part]]|Others], Parts).