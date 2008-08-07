
-module(lib_misc).

-define(OFFSET_BASIS, 2166136261).
-define(FNV_PRIME, 16777619).

-export([pmap/2, hash/1, position/2]).

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
  
position(Predicate, List) when is_function(Predicate) ->
  position(Predicate, List, 1);
  
position(E, List) ->
  position(E, List, 1).
  
position(Predicate, [], N) when is_function(Predicate) -> false;

position(Predicate, [E|List], N) when is_function(Predicate) ->
  case Predicate(E) of
    true -> N;
    false -> position(Predicate, List, N+1)
  end;
  
position(_, [], _) -> false;

position(E, [E|List], N) -> N;

position(E, [_|List], N) -> position(E, List, N+1).