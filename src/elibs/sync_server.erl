%%%-------------------------------------------------------------------
%%% File:      sync_server.erl
%%% @author    Cliff Moon <cliff@powerset.com> [http://www.powerset.com/]
%%% @copyright 2008 Cliff Moon
%%% @doc  
%%%
%%% @end  
%%%
%%% @since 2008-10-03 by Cliff Moon
%%%-------------------------------------------------------------------
-module(sync_server).
-author('cliff@powerset.com').

%% API
-export([start_link/3]).

-record(state, {name, partition, nodes}).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% @spec start_link() -> {ok,Pid} | ignore | {error,Error}
%% @doc Starts the server
%% @end 
%%--------------------------------------------------------------------
start_link(Name, Partition, Nodes) ->
  Pid = spawn_link(fun() ->
      sync_server:loop(#state{name=Name,partition=Partition,nodes=Nodes})
    end),
  register(Name, Pid),
  {ok, Pid}.

loop(State = #state{name=Name,partition=Partition,nodes=Nodes}) ->
  [NodeA,NodeB|_] = lib_misc:shuffle(Nodes),
  loop(State).
  