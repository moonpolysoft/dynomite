-include_lib("include/eunit/eunit.hrl").

dict_storage_test() ->
	storage_server:start_link(dict_storage, ok),
	storage_server:put(key, value),
	storage_server:put(two, value2),
	value = storage_server:get(key),
	true = storage_server:has_key(key),
	storage_server:delete(key),
	false = storage_server:has_key(key),
	storage_server:close().