%%%-------------------------------------------------------------------
%%% File:      dets_storage.erl
%%% @author    Cliff Moon <> []
%%% @copyright 2008 Cliff Moon
%%% @doc  
%%%
%%% @end  
%%%
%%% @since 2008-11-15 by Cliff Moon
%%%-------------------------------------------------------------------
-module(dets_storage).
-author('cliff@powerset.com').

%% API
-export([open/2, close/1, get/2, put/4, has_key/2, delete/2, fold/3]).

-record(row, {key, context, values}).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% @spec 
%% @doc
%% @end 
%%--------------------------------------------------------------------

open(Directory, Name) ->
  ok = filelib:ensure_dir(Directory ++ "/"),
  TableName = list_to_atom(lists:concat([Name, '/', node()])),
  dets:open_file(TableName, [{file, lists:concat([Directory, "/storage.dets"])}, {keypos, 2}]).
  
close(Table) -> dets:close(Table).

fold(Fun, Table, AccIn) when is_function(Fun) ->
  dets:foldl(fun(#row{key=Key,context=Context,values=Values}, Acc) ->
      Fun({Key, Context, Values}, Acc)
    end, AccIn, Table).
    
put(Key, Context, Values, Table) ->
  case dets:insert(Table, [#row{key=Key,context=Context,values=Values}]) of
    ok -> {ok, Table};
    Failure -> Failure
  end.
  
get(Key, Table) ->
  case dets:lookup(Table, Key) of
    [] -> {ok, not_found};
    [#row{context=Context,values=Values}] -> {ok, {Context, Values}}
  end.
  
has_key(Key, Table) ->
  case dets:member(Table, Key) of
    true -> {ok, true};
    false -> {ok, false};
    Failure -> Failure
  end.
  
delete(Key, Table) -> 
  case dets:delete(Table, Key) of
    ok -> {ok, Table};
    Failure -> Failure
  end.

%%====================================================================
%% Internal functions
%%====================================================================

