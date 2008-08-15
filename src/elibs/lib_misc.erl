
-module(lib_misc).

-define(OFFSET_BASIS, 2166136261).
-define(FNV_PRIME, 16777619).

-export([pmap/2, hash/1]).

pmap(Fun, List) ->
  Parent = self(),
  Pids = [spawn(fun() -> Parent ! {self(), {Elem, (catch Fun(Elem))}} end)
    || Elem <- List],
  [receive {Pid, Val} -> Val end || Pid <- Pids].
  
%32 bit fnv.  magic numbers ahoy
hash(Term) when is_binary(Term) ->
  fnv_int(?OFFSET_BASIS, Term);
  
hash(Term) ->
  fnv_int(?OFFSET_BASIS, term_to_binary(Term)).
  
fnv_int(Hash, <<"">>) -> Hash;
  
fnv_int(Hash, <<Octet:8, Bin/binary>>) ->
  Xord = Hash bxor Octet,
  fnv_int((Xord * ?FNV_PRIME) rem (2 bsl 31), Bin).