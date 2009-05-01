-module(dynomite).

-export([start/0, running/1, running_nodes/0, pause_all_sync/0, start_all_sync/0]).

-include("../include/common.hrl").

start() ->
  crypto:start(),
  load_and_start_apps([os_mon, thrift, mochiweb, dynomite]).

running(Node) when Node == node() ->
  true;

running(Node) ->
  Ref = erlang:monitor(process, {membership, Node}),
  receive
    {'DOWN', Ref, _, _, _} -> false
  after 1 ->
    erlang:demonitor(Ref),
    true
  end.

running_nodes() ->
  [Node || Node <- nodes([this,visible]), dynomite:running(Node)].

pause_all_sync() ->
  SyncServers = lists:flatten(lists:map(fun(Node) ->
      rpc:call(Node, sync_manager, loaded, [])
    end, running_nodes())),
  lists:foreach(fun(Server) ->
      sync_server:pause(Server)
    end, SyncServers).

start_all_sync() ->
  SyncServers = lists:flatten(lists:map(fun(Node) ->
      rpc:call(Node, sync_manager, loaded, [])
    end, running_nodes())),
  lists:foreach(fun(Server) ->
      sync_server:play(Server)
    end, SyncServers).

%%==============================================================

load_and_start_apps([]) ->
  ok;

load_and_start_apps([App|Apps]) ->
  case application:load(App) of
    ok ->
      case application:start(App) of
        ok -> load_and_start_apps(Apps);
        Err ->
          ?infoFmt("error starting ~p: ~p~n", [App, Err]),
          timer:sleep(10),
          halt(1)
      end;
    Err ->
      ?infoFmt("error loading ~p: ~p~n", [App, Err]),
      Err,
      timer:sleep(10),
      halt(1)
  end.

collect_loop() ->
  process_flag(trap_exit, true),
  Filename = io_lib:format("/home/cliff/dumps/~w-dyn.dump", [lib_misc:now_int()]),
  sys_info(Filename),
  receive
    nothing -> ok
  after 5000 -> collect_loop()
  end.

sys_info(Filename) ->
  {ok, IO} = file:open(Filename, [write]),
  ok = io:format(IO, "count ~p~n", [erlang:system_info(process_count)]),
  ok = io:format(IO, "memory ~p~n", [erlang:memory()]),
  ok = file:write(IO, erlang:system_info(procs)),
  file:close(IO).
