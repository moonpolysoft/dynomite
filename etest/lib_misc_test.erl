-include_lib("eunit.hrl").

pmap_test() ->
  L = [0, 1, 2],
  ?assertEqual([0,1], pmap(fun(N) ->
      timer:sleep(N),
      N
    end, L, 2)).
    
pmap_1_test() ->
  L = [0],
  ?assertEqual([0], pmap(fun(N) ->
      N
    end, L, 1)).
    
reverse_bits_test() ->
  3869426816 = reverse_bits(19088743),
  1458223569 = reverse_bits(2342344554).
