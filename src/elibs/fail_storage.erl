-module (fail_storage).
-export ([open/2, close/1, get/2, put/4, has_key/2, delete/2]).

% we ignore the name, since it can't really help us.
open(_, _) -> dict:new().

% noop
close(_Table) -> ok.

put(_Key, _Context, _Value, _Table) ->
	{failure, "Sysadmin accidentally destroyed pager with a large hammer."}.
	
get(_Key, _Table) ->
  {failure, "Groundskeepers stole the root password"}.
	
has_key(_Key, _Table) ->
	{failure, "piezo-electric interference!"}.
	
delete(_Key, _Table) ->
  {failure, "Netscape has crashed"}.