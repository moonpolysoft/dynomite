-module (dict_storage).
-export ([open/1, close/1, get/2, put/4, has_key/2, delete/2]).

% we ignore the name, since it can't really help us.
open(_) -> dict:new().

% noop
close(_Table) -> ok.

put(Key, Context, Value, Table) ->
	dict:store(Key, {Context,Value}, Table).
	
get(Key, Table) ->
	dict:fetch(Key, Table).
	
has_key(Key, Table) ->
	dict:is_key(Key, Table).
	
delete(Key, Table) ->
	dict:erase(Key, Table).