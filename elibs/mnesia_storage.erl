-module (mnesia_storage).
-export ([open/2, close/1, get/2, put/4, has_key/2, delete/2, fold/3]).

%% largest value size allowed in RAM storage
-define(MEM_VALUE_LIMIT, 1024).

-record(entry, {
          key, 
          values,
          path,
          context,
          updated
          %% expires -- someday
         }).


open(Directory, Name) ->
    ok = filelib:ensure_dir(Directory ++ "/"),
    TableName = list_to_atom(lists:concat([Name, '_', node()])),
    ok = ensure_table(TableName),
    {ok, {Directory, TableName}}.

% noop
close({_Directory, Table}) -> ok.

fold(Fun, {_Directory, Table}, AccIn) when is_function(Fun) ->
    InnerFoldFun = fun(#entry{key=Key, values=V, path=P, context=C}, Acc) ->
                           Values = case V of
                                        undefined ->
                                            read_file(P);
                                        _ ->
                                           V
                                    end,
                           Fun({Key, C, Values}, Acc)
                   end,
    FoldFun = fun() ->
                      mnesia:foldl(InnerFoldFun, AccIn, Table)
              end,
    mnesia:ets(FoldFun).

put(Key, Context, Values, {Directory, Table}) ->
    Vlist = if
                is_list(Values) -> Values;
                true -> [Values]
            end,
    Old = mnesia:ets(fun() ->
                             case mnesia:read(Table, Key, read) of
                                 [Entry] ->
                                     Entry;
                                 _ ->
                                     #entry{}
                             end
                     end),
    case value_size(Values) of
        large ->
            put_values_in_file(
              Key, Context, Vlist, Directory, Table, Old#entry.path);
        _ ->
            Write = fun() ->
                            Entry = #entry{key=Key, 
                                           context=Context, 
                                           values=Vlist,
                                           updated=erlang:now()},
                            mnesia:write(Table, Entry, write)
                    end,
            %% can't use ets access here, since we're writing
            ok = mnesia:async_dirty(Write),
            %% clean up any old path
            case Old#entry.path of
                undefined ->
                    ok;
                P when is_list(P) ->
                    ok = file:delete(P)
            end,
            {ok, {Directory, Table}}
    end.

get(Key, {_Directory, Table}) ->
    Read = fun() ->
                   mnesia:read(Table, Key, read)
           end,
    case mnesia:ets(Read) of
        [] ->
             {ok, not_found};
        [#entry{key=Key, context=C, path=P}] when is_list(P) ->
            {ok, {C, read_file(P)}};
        [#entry{key=Key, values=V, context=C}] ->
            {ok, {C, V}};
        Other ->
            Other
    end.
	
has_key(Key, {_Directory, Table}) ->
    Read = fun () ->
                   mnesia:read(Table, Key, read)
           end,
    case mnesia:ets(Read) of
        [] ->
            {ok, false};
        [_Record] ->
            {ok, true};
        Other ->
            {ok, false}
    end.
	
delete(Key, {Directory, Table}) ->
    Del = fun () ->
                  case mnesia:read(Table, Key, read) of
                      [#entry{path=P}] ->
                          {mnesia:delete(Table, Key, write), P};
                      _ ->
                          {ok, not_found}
                  end
          end,
    %% can't use ets access mode here, since we're writing
    case mnesia:async_dirty(Del) of
        {ok, not_found} ->
            {ok, {Directory, Table}};
        {ok, undefined} ->
            {ok, {Directory, Table}};
        {ok, Path} when is_list(Path) ->
            ok = file:delete(Path),
            {ok, {Directory, Table}};
        Other ->
            Other
    end.
	
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%	internal functions	
	
read_file(Path) ->
    {ok, Binary} = file:read_file(Path),
    Values = case (catch binary_to_term(Binary)) of
                 {'EXIT', _} -> [Binary];
                 Terms -> Terms
             end,
    Values.

put_values_in_file(Key, Context, Values, Directory, Table, Path) ->
    Read = fun() ->
                   mnesia:read(Table, Key, read)
           end,
    HashedFilename = case Path of
                         undefined ->
                             create_filename(Directory, Key);
                         _ ->
                             Path
                     end,
    ToWrite = term_to_binary(Values),
    Write = fun() ->
                    Entry = #entry{key=Key,
                                   context=Context,
                                   path=HashedFilename,
                                   updated=erlang:now()},
                    mnesia:write(Table, Entry, write)
            end,
    %% can't use ets access mode here, since we're writing
    ok = mnesia:async_dirty(Write),
    ok = file:write_file(HashedFilename, ToWrite, [raw]),
    {ok, {Directory, Table}}.

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

value_size(V) ->
    Len = iolist_size(V),
    if Len >= ?MEM_VALUE_LIMIT ->
            large;
       true ->
            small
    end.

ensure_table(Table) ->
    %% ensure that schema is on disc
    case mnesia:start() of
        ok ->
            ok;
        {error, already_started} ->
            ok;
        Error ->
            exit(Error)
    end,
    case mnesia:change_table_copy_type(schema, node(), disc_copies) of
        {atomic, ok} ->
            ok;
        {aborted, {already_exists, _, _, _}} ->
            ok;
        Fail ->
            exit(Fail)
    end,
    case mnesia:create_table(Table, 
                             [{record_name, entry},
                              {attributes, record_info(fields, entry)},
                              {disc_copies, [node()]},
                              {local_content, true}]) of
        {atomic, ok} ->
            ok;
        {aborted, {already_exists, _}} ->
            ok;
        Other ->
            Other
    end.
           
