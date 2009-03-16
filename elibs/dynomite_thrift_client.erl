%%%-------------------------------------------------------------------
%%% File:      dynomite_thrift_client.erl
%%% @author    Cliff Moon <cliff@powerset.com> []
%%% @copyright 2009 Cliff Moon
%%% @doc  
%%%
%%% @end  
%%%
%%% @since 2009-03-15 by Cliff Moon
%%%-------------------------------------------------------------------
-module(dynomite_thrift_client).
-author('cliff@powerset.com').

%% API
-export([start_link/2, get/2, put/4, has/2, remove/2, stop/1]).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% @spec 
%% @doc
%% @end 
%%--------------------------------------------------------------------
start_link(Host, Port) when is_integer(Port) ->
  thrift_client:start_link(Host, Port, dynomite_thrift).
  
get(C, Key) ->
  thrift_client:call(C, get, [Key]).
  
put(C, Key, Context, Value) ->
  thrift_client:call(C, put, [Key, Context, Value]).
  
remove(C, Key) ->
  thrift_client:call(C, remove, [Key]).
  
has(C, Key) ->
  thrift_client:call(C, hash, [Key]).

stop(C) ->
  thrift_client:close(C).
%%====================================================================
%% Internal functions
%%====================================================================

