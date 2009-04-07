-include_lib("eunit.hrl").

store_conflicting_versions_test() ->
  configuration:start_link(#config{}),
  {ok, Pid} = storage_server:start_link(dets_storage, db_key(confl), store, 0, (2 bsl 31), 4096),
  A = vector_clock:create(a),
  B = vector_clock:create(b),
  storage_server:put(Pid, "key", A, ["blah"]),
  storage_server:put(Pid, "key", B, ["blah2"]),
  ?assertMatch({ok, {_, ["blah", "blah2"]}}, storage_server:get(Pid, "key")),
  configuration:stop(),
  timer:sleep(1),
  storage_server:close(Pid).
  
storage_server_throughput_test_() ->
  {timeout, 500, {?LINE, fun() -> test_storage_server_throughput() end}}.
  
test_storage_server_throughput() ->
  configuration:start_link(#config{}),
  {ok, Pid} = storage_server:start_link(dets_storage, db_key(throughput), through, 0, (2 bsl 31), 4096),
  {Keys, _} = lib_misc:fast_acc(fun({List, Str}) -> 
      Mod = lib_misc:succ(Str),
      {[Mod|List], Mod}
    end, {[], "aaaaaaaa"}, 10000),
  Vector = vector_clock:create(a),
  Start = lib_misc:now_float(),
  lists:foreach(fun(Key) ->
      storage_server:put(Pid, Key, Vector, Key)
    end, Keys),
  lists:foreach(fun(Key) ->
      storage_server:get(Pid, Key)
    end, Keys),
  End = lib_misc:now_float(),
  ?debugFmt("storage server can do ~p reqs/s", [20000/(End-Start)]),
  storage_server:close(Pid).

couch_storage_test() ->
    configuration:start_link(#config{}),
    CouchFile = filename:join(priv_dir(), "couch"),
    {ok, State} = couch_storage:open(CouchFile, storage_test),
    {ok, St2} = couch_storage:put("key_one", context, <<"value one">>, State),
    {ok, St3} = couch_storage:put("key_one", context, <<"value one">>, St2),
    {ok, St4} = couch_storage:put("key_two", context, <<"value two">>, St3),
    Result = couch_storage:fold(fun({Key, Context, Value}, Acc) -> [Key|Acc] end, St4, []),
    timer:sleep(100),
    ["key_two", "key_one"] = Result,
    {ok, {context, <<"value one">>}} = couch_storage:get("key_one", St4),
    {ok, true} = couch_storage:has_key("key_one", St4),
    {ok, St5} = couch_storage:delete("key_one", St4),
    {ok, false} = couch_storage:has_key("key_one", St5),
    {ok, true} = couch_storage:has_key("key_two", St5),
    {ok, St6} = couch_storage:delete("key_two", St5),
    configuration:stop(),
    timer:sleep(1),
    couch_storage:close(St6).

dict_storage_test() ->
    configuration:start_link(#config{}),
    {ok, Pid} = storage_server:start_link(
                  dict_storage, db_key(dict), store, 0, (2 bsl 31), 4096),
    ?debugFmt("storage server at ~p", [Pid]),
    R = storage_server:put(store, "key", context, <<"value">>),
    ?debugFmt("put result ~p", [R]),
    storage_server:put(store, "two", context, <<"value2">>),
    ["two", "key"] = storage_server:fold(store, fun({Key, Context, Value}, Acc) -> [Key|Acc] end, []),
    {ok, {context, [<<"value">>]}} = storage_server:get(store, "key"),
    {ok, true} = storage_server:has_key(store, "key"),
    storage_server:delete(store, "key"),
    {ok, false} = storage_server:has_key(store, "key"),
    storage_server:close(store),
    configuration:stop(),
    timer:sleep(1),
    receive _ -> true end.

mnesia_storage_test() ->
    configuration:start_link(#config{}),
    mnesia:stop(),
    application:set_env(mnesia, dir, priv_dir()),
    {ok, Pid} = storage_server:start_link(mnesia_storage, db_key(mnesia), store2, 0, (2 bsl 31), 4096),
    storage_server:put(store2, "key_one", [], <<"value one">>),
    storage_server:put(store2, "key_two", [], <<"value two">>),
    
    {ok, {_Context, [<<"value one">>]}} = storage_server:get(store2, "key_one"),
    
    ?assertEqual(["key_one", "key_two"],
                 storage_server:fold(store2, 
                                     fun({Key, Context, Value}, Acc) -> 
                                             [Key|Acc] 
                                     end, 
                                     [])),
    
    {ok,true} = storage_server:has_key(store2, "key_one"),
    storage_server:delete(store2, "key_one"),
    {ok,false} = storage_server:has_key(store2, "key_one"),
    {ok,true} = storage_server:has_key(store2, "key_two"),
    storage_server:delete(store2, "key_two"),
    exit(Pid, shutdown),
    configuration:stop(),
    timer:sleep(1),
    receive _ -> true end.

mnesia_large_value_test() ->
    configuration:start_link(#config{}),
    crypto:start(), %% filename generation uses crypto:sha
    mnesia:stop(),
    application:set_env(mnesia, dir, priv_dir()),
    {ok, Pid} = storage_server:start_link(mnesia_storage, db_key(mnesia), store3, 0, (2 bsl 31), 4096),
    Val = big_val(2048),
    ok = storage_server:put(store3, "key_one", [], Val),
    {ok, {_Context, [Val]}} = storage_server:get(store3, "key_one"),
    configuration:stop(),
    timer:sleep(1).

% concurrent_update_test() ->
%   process_flag(trap_exit, true),
%   {ok, Pid} = storage_server:start_link(dets_storage, db_key(dets), store4, 0, (2 bsl 31), undefined),
%   Keys1 = [lists:concat(["key", V]) || V <- lists:seq(1, 1000)],
%   Keys2 = [lists:concat(["key", V]) || V <- lists:seq(1001, 2000)],
%   lists:foreach(fun(K) -> storage_server:put(store4, K, [], <<"val">>) end, Keys1),
%   P1 = spawn_link(fun() ->
%       lists:foreach(fun(K) -> 
%           storage_server:put(store4, K, [], <<"val">>) end, Keys2)
%     end),
%   P2 = spawn_link(fun() ->
%       storage_server:fold(store4, fun({Key, Ctx, Vals}, _) -> 
%           ?assert(lists:any(fun(E) -> E == Key end, Keys1))
%         end, nil)
%     end),
%   receive {'EXIT', P1, _} -> ok end,
%   receive {'EXIT', P2, _} -> ok end.
  
rebuild_merkle_trees_test() ->
  configuration:start_link(#config{}),
  {ok, _} = mock:mock(dmerkle),
  {ok, _} = mock:mock(dets_storage),
  mock:expects(dmerkle, open, fun(_) -> true end, {error, "Poop, fart. Balls."}),
  mock:expects(dmerkle, open, fun(_) -> true end, {ok, pid}),
  mock:expects(dmerkle, updatea, fun({K, V, P}) -> (K == "key") and (V == [<<"values">>]) end, fun({_, _, P}, _) -> P end),
  mock:expects(dets_storage, open, fun(_) -> true end, {ok, table}),
  mock:expects(dets_storage, fold, fun({F, A, T}) -> (T == table) and is_function(F) end, fun({F, A, T}, _) ->
      F({"key",ctx,[<<"values">>]}, A)
    end),
  {ok, Pid} = storage_server:start_link(dets_storage, db_key(merkle_test), store5, 0, (2 bsl 31), 4096),
  timer:sleep(10),
  mock:verify(dmerkle),
  mock:verify(dets_storage),
  mock:stop(dmerkle),
  mock:stop(dets_storage),
  configuration:stop(),
  timer:sleep(1),
  storage_server:close(Pid).
  
streaming_put_test() ->
  configuration:start_link(#config{}),
  {ok, _} = mock:mock(dmerkle),
  {ok, _} = mock:mock(dets_storage),
  mock:expects(dmerkle, open, fun(_) -> true end, {ok, pid}),
  mock:expects(dets_storage, open, fun(_) -> true end, {ok, table}),
  Bits = 10000 * 8,
  Bin = <<0:Bits>>,
  mock:expects(dets_storage, put, fun({_, _, [Val], table}) -> Val == Bin end, {ok, table}),
  mock:expects(dets_storage, get, fun({Key, Table}) -> Key == "key" end, {ok, not_found}),
  mock:expects(dmerkle, updatea, fun(_) -> true end, fun(_, _) -> self() end),
  {ok, Pid} = storage_server:start_link(dets_storage, db_key(merkle_test), store6, 0, (2 bsl 31), 4096),
  Result = stream(Pid, "key", ctx, Bin),
  ?debugFmt("~p", [Result]),
  ?assertEqual(ok, Result),
  mock:verify(dmerkle),
  mock:verify(dets_storage),
  mock:stop(dmerkle),
  mock:stop(dets_storage),
  configuration:stop(),
  timer:sleep(1),
  storage_server:close(Pid).
  
buffered_test_loop(Called) ->
  receive
    put -> buffered_test_loop(true);
    {called, Pid} -> 
      Pid ! {called, Called},
      buffered_test_loop(Called)
  end.
  
interrogate_test_loop(Pid) ->
  Pid ! {called, self()},
  receive
    {called, Called} -> Called
  end.
  
buffered_small_write_test() ->
  configuration:start_link(#config{buffered_writes=true}),
  {ok, _} = mock:mock(dmerkle),
  {ok, _} = mock:mock(dets_storage),
  mock:expects(dmerkle, open, fun(_) -> true end, {ok, pid}),
  mock:expects(dets_storage, open, fun(_) -> true end, {ok, table}),
  Bits = 10 * 8,
  Bin = <<0:Bits>>,
  % race conditions ahoy cap'n
  Pid = spawn(fun() -> buffered_test_loop(false) end),
  mock:expects(dets_storage, put, fun({_, _, [Val], table}) -> Val == Bin end, fun(_, _) -> 
      timer:sleep(200), %we sleep to simulate a long write so we can test that shit returns b4 write is complete
      ?debugMsg("processing put"),
      Pid ! put,        %hence a buffered write.  hopefully this won't cause the cluster to explode
      {ok, table}
    end),
  mock:expects(dets_storage, get, fun({Key, Table}) -> Key == "key" end, {ok, not_found}),
  mock:expects(dmerkle, updatea, fun(_) -> true end, fun(_, _) -> self() end),
  {ok, Store} = storage_server:start_link(dets_storage, db_key(buff_test), store7, 0, (2 bsl 31), 4096),
  int_put(Store, "key", ctx, Bin, 1000),
  ?assertEqual(false, interrogate_test_loop(Pid)),
  timer:sleep(100), %this should work, yes?  icky.
  mock:verify_and_stop(dmerkle),
  mock:verify_and_stop(dets_storage),
  ?assertEqual(true, interrogate_test_loop(Pid)),
  exit(Pid, shutdown),
  configuration:stop(),
  timer:sleep(1),
  storage_server:close(Store).
  
buffered_stream_write_test() ->
  configuration:start_link(#config{buffered_writes=true}),
  {ok, _} = mock:mock(dmerkle),
  {ok, _} = mock:mock(dets_storage),
  mock:expects(dmerkle, open, fun(_) -> true end, {ok, pid}),
  mock:expects(dets_storage, open, fun(_) -> true end, {ok, table}),
  Bits = 10000 * 8,
  Bin = <<0:Bits>>,
  % race conditions ahoy cap'n
  Pid = spawn(fun() -> buffered_test_loop(false) end),
  mock:expects(dets_storage, put, fun({_, _, [Val], table}) -> Val == Bin end, fun(_, _) -> 
      timer:sleep(200), %we sleep to simulate a long write so we can test that shit returns b4 write is complete
      ?debugMsg("processing put"),
      Pid ! put,        %hence a buffered write.  hopefully this won't cause the cluster to explode
      {ok, table}
    end),
  mock:expects(dets_storage, get, fun({Key, Table}) -> Key == "key" end, {ok, not_found}),
  mock:expects(dmerkle, updatea, fun(_) -> true end, fun(_, _) -> self() end),
  {ok, Store} = storage_server:start_link(dets_storage, db_key(buff_test), store7, 0, (2 bsl 31), 4096),
  stream(Store, "key", ctx, Bin),
  ?debugHere,
  ?assertEqual(false, interrogate_test_loop(Pid)),
  timer:sleep(100), %this should work, yes?  icky.
  mock:verify_and_stop(dmerkle),
  mock:verify_and_stop(dets_storage),
  ?assertEqual(true, interrogate_test_loop(Pid)),
  exit(Pid, shutdown),
  configuration:stop(),
  timer:sleep(1),
  storage_server:close(Store).
  
caching_test() ->
  configuration:start_link(#config{cache=true,cache_size=120}),
  {ok, _} = mock:mock(dmerkle),
  {ok, _} = mock:mock(dets_storage),
  mock:expects(dmerkle, open, fun(_) -> true end, {ok, pid}),
  mock:expects(dets_storage, open, fun(_) -> true end, {ok, table}),
  mock:expects(dets_storage, get, fun({Key, Table}) -> Key == "key" end, {ok, not_found}),
  mock:expects(dets_storage, put, fun({_, _, _, table}) -> true end, {ok, table}),
  mock:expects(dmerkle, updatea, fun(_) -> true end, fun(_, _) -> self() end),
  {ok, Store} = storage_server:start_link(dets_storage, db_key(cache_put_test), store8, 0, (2 bsl 31), 4096),
  Clock = vector_clock:create(something),
  storage_server:put(Store, "key", Clock, <<"value">>),
  Ret = storage_server:get(Store, "key"),
  ?debugFmt("ret ~p", [Ret]),
  ?assertEqual({ok, {Clock, [<<"value">>]}}, Ret),
  storage_server:close(Store).
%   
% local_fs_storage_test() ->
%   {ok, State} = fs_storage:open("/Users/cliff/data/storage_test", storage_test),
%   fs_storage:put("key_one", context, <<"value one">>, State),
%   fs_storage:put("key_one", context, <<"value one">>, State),
%   fs_storage:put("key_two", context, <<"value two">>, State),
%   ["key_two", "key_one"] = fs_storage:fold(fun({Key, Context, Value}, Acc) -> [Key|Acc] end, State, []),
%   {ok, {context, [<<"value one">>]}} = fs_storage:get("key_one", State),
%   {ok, true} = fs_storage:has_key("key_one", State),
%   fs_storage:delete("key_one", State),
%   {ok, false} = fs_storage:has_key("key_one", State),
%   {ok, true} = fs_storage:has_key("key_two", State),
%   fs_storage:delete("key_two", State),
%   fs_storage:close(State).
%   
% fs_storage_test() ->
%   {ok, Pid} = storage_server:start_link(fs_storage, "/Users/cliff/data/storage_test", store2, 0, (2 bsl 31)),
%   storage_server:put(store2, "key_one", context, <<"value one">>),
%   storage_server:put(store2, "key_one", context, <<"value one">>),
%   storage_server:put(store2, "key_two", context, <<"value two">>),
%   ["key_two", "key_one"] = storage_server:fold(store2, fun({Key, Context, Value}, Acc) -> [Key|Acc] end, []),
%   {ok, {context, [<<"value one">>]}} = storage_server:get(store2, "key_one"),
%   {ok,true} = storage_server:has_key(store2, "key_one"),
%   storage_server:delete(store2, "key_one"),
%   {ok,false} = storage_server:has_key(store2, "key_one"),
%   {ok,true} = storage_server:has_key(store2, "key_two"),
%   storage_server:delete(store2, "key_two"),
%   exit(Pid, shutdown),
%   receive _ -> true end.
%   
% sync_storage_test() ->
%   {ok, Pid} = storage_server:start_link(dict_storage, ok, store1, 0, (2 bsl 31)),
%   {ok, Pid2} = storage_server:start_link(dict_storage, ok, store2, 0, (2 bsl 31)),
%   storage_server:put(store2, "key_one", vector_clock:create(a), <<"value one">>),
%   storage_server:put(store2, "key_two", vector_clock:create(a), <<"value two">>),
%   storage_server:sync(store2, Pid),
%   {ok, {_, [<<"value one">>]}} = storage_server:get(Pid, "key_one"),
%   {ok, {_, [<<"value two">>]}} = storage_server:get(Pid, "key_two"),
%   exit(Pid, shutdown),
%   receive _ -> true end,
%   exit(Pid2, shutdown),
%   receive _ -> true end.


%% Internal
priv_dir() ->
    Dir = filename:join(t:config(priv_dir), "data"),
    filelib:ensure_dir(filename:join(Dir, "couch")),
    Dir.

db_key(Name) ->
    Fpath =  filename:join(
               [t:config(priv_dir), "storage_server", atom_to_list(Name)]),
    filelib:ensure_dir(filename:join(Fpath, "dmerkle")),
    Fpath.
    
merkle_file(Name) ->
  filename:join(db_key(Name), "dmerkle.idx").

big_val(Size) when is_integer(Size) ->
    list_to_binary([ random:uniform(128) || _ <- lists:seq(0, Size) ]).
    
