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
-export([info/1, rates/1]).

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
    {member_nodes,transform_partitions([], lists:keysort(1, membership:partitions()))}
  ]}.

rates(cluster) ->
  {obj, [
    {get_rate, lists:foldl(fun(Node, Acc) -> Acc + socket_server:rate(Node, get_rate, 1) end, 0, nodes([this,visible]))},
    {put_rate, lists:foldl(fun(Node, Acc) -> Acc + socket_server:rate(Node, put_rate, 1) end, 0, nodes([this,visible]))},
    {in_rate, lists:foldl(fun(Node, Acc) -> Acc + socket_server:rate(Node, in_rate, 1) end, 0, nodes([this,visible]))},
    {out_rate, lists:foldl(fun(Node, Acc) -> Acc + socket_server:rate(Node, out_rate, 1) end, 0, nodes([this,visible]))},
    {connections, lists:foldl(fun(Node, Acc) -> Acc + socket_server:connections(Node) end, 0, nodes([this,visible]))}
  ]};

rates(Node) ->
  {obj, [
    {get_rate, socket_server:rate(Node, get_rate, 1)},
    {put_rate, socket_server:rate(Node, put_rate, 1)},
    {in_rate, socket_server:rate(Node, in_rate, 1)},
    {out_rate, socket_server:rate(Node, out_rate, 1)},
    {connections, socket_server:connections(Node)}
  ]}.

%%====================================================================
%% Internal functions
%%====================================================================

transform_partitions([], [{Node,Part}|Parts]) ->
  transform_partitions([[Node,[Part]]], Parts);
  
transform_partitions(NodeParts, []) ->
  [[Node, lists:sort(Parts)] || [Node, Parts] <- lists:sort(NodeParts)]; 
  
transform_partitions([[Node,NodeParts]|Others], [{Node,Part}|Parts]) ->
  transform_partitions([[Node,[Part|NodeParts]]|Others], Parts);
  
transform_partitions(Others, [{Node,Part}|Parts]) ->
  transform_partitions([[Node,[Part]]|Others], Parts).