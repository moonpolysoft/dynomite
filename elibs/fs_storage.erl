-module (fs_storage).
-export ([open/2, close/1, get/2, put/4, has_key/2, delete/2, fold/3]).

-record(file, {
  name,
  path,
  context
}).

-record(row, {key, context, values}).

% open with the name of the fs directory
open(Directory, Name) ->
  ok = filelib:ensure_dir(Directory ++ "/"),
  TableName = list_to_atom(lists:concat([Name, '/', node()])),
  SmallTableName = list_to_atom(lists:concat([Name, "/data/", node()])),
  {ok, FileTable} = dets:open_file(TableName, [{file, lists:concat([Directory, "/files.dets"])}, {keypos, 2}]),
  {ok, SmallTable} = dets:open_file(SmallTableName, [{file, lists:concat([Directory, "/small_data.dets"])}, {keypos, 2}]),
  {ok, {Directory, FileTable, SmallTable}}.

% noop
close({_Directory, FileTable, SmallTable}) -> 
  dets:close(FileTable),
  dets:close(SmallTable).

fold(Fun, {_Directory, FileTable, SmallTable}, AccIn) when is_function(Fun) ->
  Acc2 = dets:foldl(fun(#file{name=Key,path=Path,context=Context}, Acc) ->
      {ok, Value} = file:read_file(Path),
      Fun({Key, Context, Value}, Acc)
    end, AccIn, FileTable),
  dets:foldl(fun(#row{key=Key,context=Context,values=Values}, Acc) ->
      Fun({Key, Context, Values}, Acc)
    end, Acc2, SmallTable).

put(Key, Context, Values, TableStruct) ->
  case iolist_size(Values) of
    Size when Size < 10000 -> small_size_put(Key, Context, Values, TableStruct);
    _ -> big_size_put(Key, Context, Values, TableStruct)
  end.
  
big_size_put(Key, Context, Values, {Directory, FileTable, SmallTable}) ->
  case dets:lookup(FileTable, Key) of
    [Record] -> #file{path=HashedFilename} = Record;
    [] -> HashedFilename = create_filename(Directory, Key),
      _Record = not_found
  end,
  {ok, Io} = file:open(HashedFilename, [write]),
  ToWrite = if
    is_list(Values) -> term_to_binary(Values);
    true -> term_to_binary([Values])
  end,
  ok = file:write(Io, ToWrite),
  ok = file:close(Io),
  ok = dets:insert(FileTable, [#file{name=Key, path=HashedFilename, context=Context}]),
  {ok, {Directory, FileTable, SmallTable}}.
  
small_size_put(Key, Context, Values, {Directory, FileTable, SmallTable}) ->
  ok = dets:insert(SmallTable, [#row{key=Key,context=Context,values=Values}]),
  {ok, {Directory, FileTable, SmallTable}}.
	
get(Key, {_Directory, FileTable, SmallTable}) ->
  case dets:lookup(FileTable, Key) of
	  [] ->
	    case dets:lookup(SmallTable, Key) of
	      [] -> {ok, not_found};
	      [#row{key=Key,context=Context,values=Values}] ->
	        {ok, {Context, Values}}
      end;
	  [#file{path=Path,context=Context}] -> 
	    {ok, Binary} = file:read_file(Path),
	    Values = case (catch binary_to_term(Binary)) of
	      {'EXIT', _} -> [Binary];
	      Terms -> Terms
      end,
	    {ok, {Context, Values}}
  end.
	
has_key(Key, {_Directory, FileTable, SmallTable}) ->
  case dets:lookup(FileTable, Key) of
    [] ->
      case dets:lookup(SmallTable, Key) of
        [] -> {ok, false};
        _ -> {ok, true}
      end;
    [_Record] -> {ok, true}
  end.
	
delete(Key, TS = {Directory, FileTable, SmallTable}) ->
	case dets:lookup(FileTable, Key) of
	  [] ->
	    case dets:lookup(SmallTable, Key) of
	      [] -> {ok, TS};
	      _ ->
	        ok = dets:delete(SmallTable, Key),
	        {ok, TS}
      end;
	  [#file{path=Path}] ->
	    ok = file:delete(Path),
	    ok = dets:delete(FileTable, Key),
	    {ok, TS}
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
    true -> hash_to_directory(Directory, Hash) ++ "-" ++ integer_to_list(Append);
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
