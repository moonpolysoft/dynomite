
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
  fnv_int(?OFFSET_BASIS, 0, Term);
  
hash(Term) ->
  fnv_int(?OFFSET_BASIS, 0, term_to_binary(Term)).
  
fnv_int(Hash, ByteOffset, Bin) when byte_size(Bin) == ByteOffset ->
  Hash;
  
fnv_int(Hash, ByteOffset, Bin) ->
  <<_:ByteOffset/binary, Octet:8, _/binary>> = Bin,
  Xord = Hash bxor Octet,
  fnv_int((Xord * ?FNV_PRIME) rem (2 bsl 31), ByteOffset+1, Bin).