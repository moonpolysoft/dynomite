-module(dynomite).

-export([start/0]).

start() ->
  application:load(os_mon),
  application:start(os_mon),
  application:load(dynomite),
  % spawn(fun() -> collect_loop() end),
  crypto:start(),
  murmur:start(),
  fnv:start(),
  application:start(dynomite).
  
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