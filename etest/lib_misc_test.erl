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

nthdelete_test() ->
  A = [1,2,3,4,5],
  ?assertEqual([1,2,3,4,5], nthdelete(0, A)),
  ?assertEqual([1,2,3,4,5], nthdelete(6, A)),
  ?assertEqual([2,3,4,5], nthdelete(1, A)),
  ?assertEqual([1,2,4,5], nthdelete(3, A)).
  
zero_split_test() ->
  ?assertEqual({<<"">>, <<0,"abcdefg">>}, zero_split(<<0, "abcdefg">>)),
  ?assertEqual({<<"abd">>, <<0, "efg">>}, zero_split(<<"abd", 0, "efg">>)),
  ?assertEqual({<<"abcdefg">>, <<0>>}, zero_split(<<"abcdefg",0>>)),
  ?assertEqual(<<"abcdefg">>, zero_split(<<"abcdefg">>)).
  
  
hash_throughput_test_() ->
  {timeout, 120, [{?LINE, fun() ->
    Keys = lists:map(fun(N) ->
        lists:duplicate(1000, random:uniform(255))
      end, lists:seq(1,1000)),
    FNVStart = now_float(),
    lists:foreach(fun(Key) ->
        fnv(Key)
      end, Keys),
    FNVEnd = now_float(),
    ?debugFmt("fnv took ~ps~n", [FNVEnd - FNVStart]),
    MStart = now_float(),
    lists:foreach(fun(Key) ->
        hash(Key)
      end, Keys),
    MEnd = now_float(),
    ?debugFmt("murmur took ~ps~n", [MEnd - MStart]),
    FNVNStart = now_float(),
    lists:foreach(fun(Key) ->
        fnv:hash(Key)
      end, Keys),
    FNVNEnd = now_float(),
    ?debugFmt("fnv native took ~ps~n", [FNVNEnd - FNVNStart])
  end}]}.
  
fnv_native_compat_test() ->
  ?assertEqual(fnv("blah"), fnv:hash("blah")),
  ?assertEqual(fnv(<<"blah">>), fnv:hash(<<"blah">>)),
  ?assertEqual(fnv([<<"blah">>, "bleg"]), fnv:hash([<<"blah">>, "bleg"])).
  
rm_rf_test() ->
  lists:foldl(fun(N, Dir) ->
      NewDir = filename:join(Dir, N),
      File = filename:join(NewDir, "file"),
      filelib:ensure_dir(File),
      file:write_file(File, "blahblah"),
      NewDir
    end, priv_dir(), ["a", "b", "c", "d", "e"]),
  rm_rf(filename:join(priv_dir(), "a")),
  ?assertEqual({ok, []}, file:list_dir(priv_dir())).
  
priv_dir() ->
  Dir = filename:join([t:config(priv_dir), "lib_misc"]),
  filelib:ensure_dir(filename:join([Dir, "lib_misc"])),
  Dir.