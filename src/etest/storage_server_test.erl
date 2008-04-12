-include_lib("include/eunit/eunit.hrl").

dict_storage_test() ->
	storage_server:start_link(dict_storage, ok, store),
	storage_server:put(store, key, value),
	storage_server:put(store, two, value2),
	value = storage_server:get(store, key),
	true = storage_server:has_key(store, key),
	storage_server:delete(store, key),
	false = storage_server:has_key(store, key),
	storage_server:close(store).