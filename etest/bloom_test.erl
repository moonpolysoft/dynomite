-include_lib("eunit/include/eunit.hrl").

simple_bloom_test() ->
  file:delete(data_file()),
  {ok, Bloom} = bloom:start(data_file(), 10000, 0.001),
  bloom:put(Bloom, "wut"),
  ?assertEqual(true, bloom:has(Bloom, "wut")),
  ?assertEqual(false, bloom:has(Bloom, "fuck")),
  bloom:stop(Bloom).

insert_many_things_test() ->
  file:delete(data_file()),
  {ok, Bloom} = bloom:start(data_file(), 10000, 0.001),
  Keys = lists:map(fun(N) ->
      Key = "Key" ++ float_to_list(random:uniform()),
      bloom:put(Bloom, Key),
      Key
    end, lists:seq(1, 10000)),
  lists:foreach(fun(Key) ->
      ?assert(bloom:has(Bloom, Key))
    end, Keys),
  bloom:stop(Bloom).

false_positive_error_rate_test() ->
  file:delete(data_file()),
  {ok, Bloom} = bloom:start(data_file(), 10000, 0.001),
  lists:foreach(fun(N) ->
      Key = "Key" ++ float_to_list(random:uniform()),
      bloom:put(Bloom, Key)
    end, lists:seq(1, 10000)),
  FalsePositives = [X || X <- [bloom:has(Bloom, "butt" ++ float_to_list(random:uniform())) || N <- lists:seq(1,10000)], X == true],
  FPRate = length(FalsePositives) / 10000,
  ?debugFmt("false positives: ~p", [length(FalsePositives)]),
  ?debugFmt("false positives: ~p", [FPRate]),
  ?debugFmt("mem size ~p", [bloom:mem_size(Bloom)]),
  ?assert(FPRate < 0.001),
  ?assertEqual(10000, bloom:key_size(Bloom)),
  bloom:stop(Bloom).
  
priv_dir() ->
  Dir = filename:join(t:config(priv_dir), "data"),
  filelib:ensure_dir(filename:join(Dir, "bloom")),
  Dir.

data_file() ->
  filename:join(priv_dir(), "bloom").

data_file(N) ->
  filename:join(priv_dir(), "bloom" ++ integer_to_list(N)).