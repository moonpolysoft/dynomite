-module(dynomite).

-export([start/0]).

start() ->
  application:load(dynomite),
  case application:get_key(dynomite, node) of
    undefined -> true;
    {ok, Node} -> net_adm:ping(Node)
  end,
  application:start(dynomite).