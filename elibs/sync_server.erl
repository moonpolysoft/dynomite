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
-export([start_link/2, pause/1, play/1, loop/1]).

-record(state, {name, partition, paused}).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% @spec start_link() -> {ok,Pid} | ignore | {error,Error}
%% @doc Starts the server
%% @end 
%%--------------------------------------------------------------------
start_link(Name, Partition) ->
  Pid = proc_lib:spawn_link(fun() ->
      sync_server:loop(#state{name=Name,partition=Partition,paused=false})
    end),
  register(Name, Pid),
  {ok, Pid}.

pause(Server) ->
  Server ! pause.
  
play(Server) ->
  Server ! play.

%% Internal functions

loop(State = #state{name=Name,partition=Partition,paused=Paused}) ->
  Timeout = round((random:uniform() * 2 + 3) * 60000),
  Paused1 = receive
    pause -> true;
    play -> false
  after Timeout -> 
    Paused
  end,
  if
    Paused -> ok;
    true ->
      Nodes = membership:nodes_for_partition(Partition),
      (catch run_sync(Nodes, Partition))
  end,
  sync_server:loop(State#state{paused=Paused1}).

run_sync(Nodes, _) when length(Nodes) == 1 ->
  noop;
  
run_sync(Nodes, Partition) ->
  [Master|_] = Nodes,
  [NodeA,NodeB|_] = lib_misc:shuffle(Nodes),
  StorageName = list_to_atom(lists:concat([storage_, Partition])),
  KeyDiff = storage_server:diff({StorageName, NodeA}, {StorageName, NodeB}),
  sync_manager:sync(Partition, Master, NodeA, NodeB, length(KeyDiff)),
  storage_server:sync({StorageName, NodeA}, {StorageName, NodeB}),
  sync_manager:done(Partition).
  