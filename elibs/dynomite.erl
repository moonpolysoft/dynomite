-module(dynomite).

-export([start/0]).

start() ->
  application:load(os_mon),
  application:start(os_mon),
  application:load(dynomite),
  crypto:start(),
  application:start(dynomite).