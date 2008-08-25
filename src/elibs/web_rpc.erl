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
-export([info/1]).

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
    {running_nodes,nodes([this,visible])},
    {member_nodes,transform_partitions([], lists:keysort(1, membership:partitions()))}
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