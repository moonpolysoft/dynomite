-module (dict_storage).
-export ([open/2, close/1, get/2, put/4, has_key/2, delete/2, info/1]).

% we ignore the name, since it can't really help us.
open(_, _) -> dict:new().

% noop
close(_Table) -> ok.

info(Table) -> dict:fetch_keys(Table).

put(Key, Context, Value, Table) ->
	{ok, dict:store(Key, {Context,[Value]}, Table)}.
	
get(Key, Table) ->
  case dict:find(Key, Table) of
    {ok, Value} -> {ok, Value};
    _ -> {ok, not_found}
  end.
	
has_key(Key, Table) ->
	{ok, dict:is_key(Key, Table)}.
	
delete(Key, Table) ->
	{ok, dict:erase(Key, Table)}.