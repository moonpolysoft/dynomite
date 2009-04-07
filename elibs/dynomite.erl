-module(dynomite).

-export([start/0]).

-include("common.hrl").

start() ->
  crypto:start(),
  load_and_start_apps([os_mon, thrift, mochiweb, dynomite]).
  
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