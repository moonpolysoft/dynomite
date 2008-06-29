-include_lib("include/eunit/eunit.hrl").

dict_storage_test() ->
  storage_server:start_link(dict_storage, ok, store),
	storage_server:put(store, key, value),
	storage_server:put(store, two, value2),
	{ok, value} = storage_server:get(store, key),
	{ok, true} = storage_server:has_key(store, key),
	storage_server:delete(store, key),
	{ok, false} = storage_server:has_key(store, key),
	storage_server:close(store).
	
local_fs_storage_test() ->
  State = fs_storage:open("/Users/cliff/data/storage_test"),
  fs_storage:put("key_one", <<"value one">>, State),
  fs_storage:put("key_two", <<"value two">>, State),
  <<"value one">> = fs_storage:get("key_one", State),
  true = fs_storage:has_key("key_one", State),
  fs_storage:delete("key_one", State),
  false = fs_storage:has_key("key_one", State),
  true = fs_storage:has_key("key_two", State),
  fs_storage:delete("key_two", State),
  fs_storage:close(State).
	
fs_storage_test() ->
  storage_server:start_link(fs_storage, "/Users/cliff/data/storage_test", store2),
  storage_server:put(store2, "key_one", <<"value one">>),
  storage_server:put(store2, "key_two", <<"value two">>),
  {ok, <<"value one">>} = storage_server:get(store2, "key_one"),
  {ok,true} = storage_server:has_key(store2, "key_one"),
  storage_server:delete(store2, "key_one"),
  {ok,false} = storage_server:has_key(store2, "key_one"),
  {ok,true} = storage_server:has_key(store2, "key_two"),
  storage_server:delete(store2, "key_two"),
  storage_server:close(store2).