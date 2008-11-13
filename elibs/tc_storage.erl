%%%-------------------------------------------------------------------
%%% File:      tc_storage.erl
%%% @author    Cliff Moon <cliff@powerset.com> []
%%% @copyright 2008 Cliff Moon
%%% @doc  
%%%
%%% @end  
%%%
%%% @since 2008-10-26 by Cliff Moon
%%%-------------------------------------------------------------------
-module(tc_storage).
-author('cliff@powerset.com').

%% API
-export([open/2, close/1, get/2, put/4, has_key/2, delete/2, fold/3]).

-record(row, {key, context, value}).

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
  FileName = lists:concat([Directory, "/", Name]),
  case ensure_tcerl_start() of
    {error, Reason} -> {error, Reason};
    ok -> tcbdbets:open_file([{file, FileName}, {type, ordered_set}, {keypos, 2}])
  end.
  
close(DBHandle) ->
  tcbdbets:close(DBHandle).
  
get(Key, DBHandle) ->
  case tcbdbets:lookup(DBHandle, Key) of
    [#row{context=Context,value=Value}] -> {ok, {Context, Value}};
    [] -> {ok, not_found}
  end.
  
put(Key, Context, Value, DBHandle) ->
  ToPut = if
    is_list(Value) -> Value;
    true -> [Value]
  end,
  case tcbdbets:insert(DBHandle, [#row{key=Key,context=Context,value=ToPut}]) of
    ok ->
      tcbdbets:sync(DBHandle),
      {ok, DBHandle};
    {error, Reason} -> {error, Reason}
  end.
  
has_key(Key, DBHandle) ->
  case tcbdbets:member(DBHandle, Key) of
    true -> {ok, true};
    false -> {ok, false};
    {error, Reason} -> {error, Reason}
  end.
  
delete(Key, DBHandle) ->
  case tcbdbets:delete(DBHandle, Key) of
    ok -> {ok, DBHandle};
    {error, Reason} -> {error, Reason}
  end.
  
fold(Fun, DBHandle, AccIn) when is_function(Fun) ->
  tcbdbets:foldl(fun(#row{key=Key,context=Context,value=Value}, Acc) -> 
      Fun({Key, Context, Value}, Acc)
    end, AccIn, DBHandle).

%%====================================================================
%% Internal functions
%%====================================================================
ensure_tcerl_start() ->
  case tcerl:start() of
    ok -> ok;
    {error, {already_started,tcerl}} -> ok;
    {error, Reason} -> {error, Reason}
  end.