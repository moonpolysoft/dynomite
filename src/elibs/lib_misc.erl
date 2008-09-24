
-module(lib_misc).

-define(OFFSET_BASIS, 2166136261).
-define(FNV_PRIME, 16777619).

-export([pmap/3, hash/1, position/2]).

-ifdef(TEST).
-include("etest/lib_misc_test.erl").
-endif.

pmap(Fun, List, ReturnNum) ->
  N = if
    ReturnNum > length(List) -> length(List);
    true -> ReturnNum
  end,
  SuperParent = self(),
  SuperRef = erlang:make_ref(),
  Ref = erlang:make_ref(),
  %% we spawn an intermediary to collect the results
  %% this is so that there will be no leaked messages sitting in our mailbox
  Parent = spawn(fun() ->
      L = gather(N, Ref, []),
      SuperParent ! {SuperRef, L}
    end),
  _Pids = [spawn(fun() -> Parent ! {Ref, {Elem, (catch Fun(Elem))}} end)
    || Elem <- List],
  receive
    {SuperRef, Ret} -> Ret
  end.
  
gather(0, _, L) -> L;
gather(N, Ref, L) ->
  receive
	  {Ref, Ret} -> gather(N-1, Ref, [Ret|L])
  end.

  
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