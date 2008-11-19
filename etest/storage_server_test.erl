-include_lib("eunit.hrl").

couch_storage_test() ->
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
    couch_storage:close(St6).

dict_storage_test() ->
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
    receive _ -> true end.

mnesia_storage_test() ->
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
    receive _ -> true end.

mnesia_large_value_test() ->
    crypto:start(), %% filename generation uses crypto:sha
    mnesia:stop(),
    application:set_env(mnesia, dir, priv_dir()),
    {ok, Pid} = storage_server:start_link(mnesia_storage, db_key(mnesia), store3, 0, (2 bsl 31), 4096),
    Val = big_val(2048),
    ok = storage_server:put(store3, "key_one", [], Val),
    {ok, {_Context, [Val]}} = storage_server:get(store3, "key_one").

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

big_val(Size) when is_integer(Size) ->
    list_to_binary([ random:uniform(128) || _ <- lists:seq(0, Size) ]).
    
