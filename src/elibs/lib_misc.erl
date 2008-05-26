
-module(lib_misc).
-export([pmap/3]).

pmap(Fun, List, Nodes) ->
  SpawnFun =
    case length(Nodes) of
       0 -> fun spawn/1;
       Length ->
         NextNode = fun() -> lists:nth(random:uniform(Length), Nodes) end,
         fun(F) -> spawn(NextNode(), F) end
    end,
  Parent = self(),
  Pids = [SpawnFun(fun() -> Parent ! {self(), (catch Fun(Elem))} end)
    || Elem <- List],
  [receive {Pid, Val} -> Val end || Pid <- Pids].
