-module (fs_storage).
-export ([open/1, close/1, get/2, put/3, has_key/2, delete/2, create_filename/2]).

-record(file, {
  name,
  path
}).

% open with the name of the fs directory
open(Directory) ->
  ok = filelib:ensure_dir(Directory ++ "/"),
  TableName = list_to_atom(lists:concat([file, '/', node()])),
  {ok, TableName} = dets:open_file(TableName, [{file, lists:concat([Directory, "/files.dets"])}, {keypos, 2}]),
  ok = crypto:start(),
  {Directory, TableName}.

% noop
close({_Directory, Table}) -> dets:close(Table), crypto:stop().

put(Key, Value, {Directory, Table}) ->
  case dets:lookup(Table, Key) of
    [Record] -> #file{path=HashedFilename} = Record;
    [] -> HashedFilename = create_filename(Directory, Key),
      Record = not_found
  end,
  {ok, Io} = file:open(HashedFilename, [write]),
  ok = file:write(Io, Value),
  ok = file:close(Io),
	case Record of
	  #file{_='_'} -> noop;
	  _ -> dets:insert(Table, [#file{name=Key, path=HashedFilename}])
  end,
  {Directory, Table}.
	
	
get(Key, {_Directory, Table}) ->
  case dets:lookup(Table, Key) of
	  [] -> not_found;
	  [#file{path=Path}] -> 
	    {ok, Binary} = file:read_file(Path),
	    Binary
  end.
	
has_key(Key, {_Directory, Table}) ->
  case dets:lookup(Table, Key) of
    [] -> false;
    [_Record] -> true
  end.
	
delete(Key, {Directory, Table}) ->
	case dets:lookup(Table, Key) of
	  [] -> {Directory, Table};
	  [#file{path=Path}] ->
	    ok = file:delete(Path),
	    ok = dets:delete(Table, Key),
	    {Directory, Table}
  end.
	
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%	internal functions	
	
create_filename(Directory, Key) ->
  Hash = lists:concat(
    lists:map(
      fun(Int) -> erlang:integer_to_list(Int, 16) end, 
      binary_to_list(
        crypto:sha(
          list_to_binary(Key))))),
  Filename = ensure_against_collisions(Directory, Hash),
  ok = filelib:ensure_dir(Filename),
  Filename.
  
ensure_against_collisions(Directory, Hash) ->
  ensure_against_collisions(Directory, Hash, 0).
  
ensure_against_collisions(Directory, Hash, Append) ->
  Filename = case Append > 0 of
    true -> hash_to_directory(Directory, Hash) ++ '-' ++ integer_to_list(Append);
    false -> hash_to_directory(Directory, Hash)
  end,
  case filelib:is_file(Filename) of
    true -> ensure_against_collisions(Directory, Hash, Append+1);
    false -> Filename
  end.
  
hash_to_directory(Directory, Hash) ->
  hash_to_directory(Directory, Hash, Hash, 3).
  
hash_to_directory(Directory, _Left, Original, 0) ->
  lists:concat([Directory, '/', Original]);
  
hash_to_directory(Directory, [Char|Left], Original, Depth) ->
  hash_to_directory(lists:concat([Directory, '/', [Char]]), Left, Original, Depth-1).
