-include_lib("include/eunit/eunit.hrl").

dict_storage_test() ->
  storage_server:start_link(dict_storage, ok, store, 0, (2 bsl 31)),
  storage_server:put(store, key, context, value),
  storage_server:put(store, two, context, value2),
  ["two", "key"] = storage_server:fold(store, fun({Key, Context, Value}, Acc) -> [Key|Acc] end, []),
  {ok, {context, [value]}} = storage_server:get(store, key),
  {ok, true} = storage_server:has_key(store, key),
  storage_server:delete(store, key),
  {ok, false} = storage_server:has_key(store, key),
  storage_server:close(store).
  
local_fs_storage_test() ->
  State = fs_storage:open("/Users/cliff/data/storage_test", storage_test),
  fs_storage:put("key_one", context, <<"value one">>, State),
  fs_storage:put("key_one", context, <<"value one">>, State),
  fs_storage:put("key_two", context, <<"value two">>, State),
  ["key_two", "key_one"] = fs_storage:fold(fun({Key, Context, Value}, Acc) -> [Key|Acc] end, State, []),
  {ok, {context, [<<"value one">>]}} = fs_storage:get("key_one", State),
  {ok, true} = fs_storage:has_key("key_one", State),
  fs_storage:delete("key_one", State),
  {ok, false} = fs_storage:has_key("key_one", State),
  {ok, true} = fs_storage:has_key("key_two", State),
  fs_storage:delete("key_two", State),
  fs_storage:close(State).
	
fs_storage_test() ->
  {ok, Pid} = storage_server:start_link(fs_storage, "/Users/cliff/data/storage_test", store2, 0, (2 bsl 31)),
  storage_server:put(store2, "key_one", context, <<"value one">>),
  storage_server:put(store2, "key_one", context, <<"value one">>),
  storage_server:put(store2, "key_two", context, <<"value two">>),
  ["key_two", "key_one"] = storage_server:fold(store2, fun({Key, Context, Value}, Acc) -> [Key|Acc] end, []),
  {ok, {context, [<<"value one">>]}} = storage_server:get(store2, "key_one"),
  {ok,true} = storage_server:has_key(store2, "key_one"),
  storage_server:delete(store2, "key_one"),
  {ok,false} = storage_server:has_key(store2, "key_one"),
  {ok,true} = storage_server:has_key(store2, "key_two"),
  storage_server:delete(store2, "key_two"),
  exit(Pid, shutdown),
  receive
    _ -> true
  end.