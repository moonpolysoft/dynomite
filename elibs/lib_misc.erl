
-module(lib_misc).
-export([pmap/2]).

pmap(Fun, List) ->
  Parent = self(),
  Pids = [spawn(fun() -> Parent ! {self(), {Elem, (catch Fun(Elem))}} end)
    || Elem <- List],
  [receive {Pid, Val} -> Val end || Pid <- Pids].
