
-module(lib_misc).

-define(OFFSET_BASIS, 2166136261).
-define(FNV_PRIME, 16777619).

-export([pmap/3, hash/1, position/2, shuffle/1, floor/1, ceiling/1, time_to_epoch_int/1, time_to_epoch_float/1, now_int/0, now_float/0, byte_size/1, listify/1]).

-ifdef(TEST).
-include("etest/lib_misc_test.erl").
-endif.

floor(X) ->
  T = erlang:trunc(X),
  case (X - T) of
    Neg when Neg < 0 -> T - 1;
    Pos when Pos > 0 -> T;
    _ -> T
  end.

ceiling(X) ->
  T = erlang:trunc(X),
  case (X - T) of
    Neg when Neg < 0 -> T;
    Pos when Pos > 0 -> T + 1;
    _ -> T
  end.

shuffle(List) ->
  lists:sort(fun(A,B) ->
      case random:uniform() of
        R when R > 0.5 -> true;
        _ -> false
      end
    end, List).

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
      L = gather(N, length(List), Ref, []),
      SuperParent ! {SuperRef, L}
    end),
  _Pids = [spawn(fun() -> 
      Parent ! {Ref, {Elem, (catch Fun(Elem))}} 
    end) || Elem <- List],
  receive
    {SuperRef, Ret} -> Ret
  end.
  
gather(_, Max, _, L) when length(L) == Max -> L;
gather(0, _, _, L) -> L;
gather(N, Max, Ref, L) ->
  receive
    {Ref, {Elem, {'EXIT', Ret}}} -> gather(N, Max, Ref, [{Elem, {'EXIT', Ret}}|L]);
	  {Ref, Ret} -> gather(N-1, Max, Ref, [Ret|L])
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

now_int() ->
  time_to_epoch_int(now()).
  
now_float() ->
  time_to_epoch_float(now()).

time_to_epoch_int(Time) when is_integer(Time) or is_float(Time) ->
  Time;

time_to_epoch_int({Mega,Sec,_}) ->
  Mega * 1000000 + Sec.
  
time_to_epoch_float(Time) when is_integer(Time) or is_float(Time) ->
  Time;
  
time_to_epoch_float({Mega,Sec,Micro}) ->
  Mega * 1000000 + Sec + Micro / 1000000.
  
byte_size(List) when is_list(List) ->
  lists:foldl(fun(El, Acc) -> Acc + byte_size(El) end, 0, List);
  
byte_size(Term) ->
  erlang:byte_size(Term).
  
listify(List) when is_list(List) ->
  List;
  
listify(El) -> [El].