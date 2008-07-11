-module(dynomite).

-export([start/0]).

start() ->
  application:load(dynomite),
  application:start(dynomite).