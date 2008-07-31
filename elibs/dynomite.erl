-module(dynomite).

-export([start/0]).

start() ->
  application:load(dynomite),
  crypto:start(),
  application:start(dynomite).