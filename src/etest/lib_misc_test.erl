-include_lib("eunit.hrl").

pmap_test() ->
  L = [0, 1, 2],
  [{1,1},{0,0}] = pmap(fun(N) ->
      timer:sleep(N),
      N
    end, L, 2).