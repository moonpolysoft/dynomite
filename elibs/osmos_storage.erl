%%%-------------------------------------------------------------------
%%% File:      osmos_storage.erl
%%% @author    cliff <cliff@moonpolysoft.com> []
%%% @copyright 2009 cliff
%%% @doc  
%%%
%%% @end  
%%%
%%% @since 2009-07-01 by cliff
%%%-------------------------------------------------------------------
-module(osmos_storage).
-author('cliff@moonpolysoft.com').


%% API
-export([open/2, close/1, get/2, put/4, has_key/2, delete/2, fold/3]).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% @spec open(Directory::string(), Name::string()) -> {ok, Table} | {error, Reason}
%% @doc
%% @end 
%%--------------------------------------------------------------------
open(Directory, Name) ->
  osmos:start(),
  TableName = list_to_atom(lists:concat([Name, '/', node()])),
  TableFormat = {osmos_table_format,
    4096, %block_size
    {osmos_format, fun list_to_binary/1, fun binary_to_list/1},
    fun osmos_keyless/2, %key_less
    {osmos_format, fun term_to_binary/1, fun binary_to_term/1},
    fun osmos_merge/3,
    fun osmos_short_circuit/2,
    fun osmos_delete/2
  },
  osmos:open(TableName, [
    {directory, Directory},
    {format, TableFormat}]).

%%--------------------------------------------------------------------
%% @spec put(Key::string(), Context::context(), Value::list(), Table) -> 
%%    {ok, Table} | {error, Reason}
%% @doc
%% @end 
%%--------------------------------------------------------------------
put(Key, Context, Value, Table) ->
  case osmos:write(Table, Key, {Context, Value}) of
    ok -> {ok, Table};
    V -> V
  end.
  
%%--------------------------------------------------------------------
%% @spec get(Key::string(), Table) -> 
%%    {ok, not_found} | {ok, {Context, Values}} | {error, Reason}
%% @doc
%% @end 
%%--------------------------------------------------------------------
get(Key, Table) ->
  case osmos:read(Table, Key) of
    not_found -> {ok, not_found};
    V -> V
  end.

%%--------------------------------------------------------------------
%% @spec has_key(Key::string(), Table) -> 
%%    {ok, true} | {ok, false} | {error, Reason}
%% @doc
%% @end 
%%--------------------------------------------------------------------
has_key(Key, Table) ->
  case osmos:read(Table, Key) of
    not_found -> {ok, false};
    {error, Reason} -> {error, Reason};
    {ok, _} -> {ok, true}
  end.
  
%%--------------------------------------------------------------------
%% @spec delete(Key::string(), Table) -> 
%%    {ok, Table} | {error, Reason}
%% @doc
%% @end 
%%--------------------------------------------------------------------
delete(Key, Table) ->
   case osmos:write(Table, Key, deleted) of
     ok -> {ok, Table};
     V -> V
   end.

%%--------------------------------------------------------------------
%% @spec close(Table) ->ok
%% @doc
%% @end 
%%--------------------------------------------------------------------
close(Table) ->
  osmos:close(Table).
   
   
fold(Fun, Table, AccIn) when is_function(Fun) ->
  case osmos:select_range(Table, fun(_) -> false end, fun(_) -> true end, fun(_, _) -> true end, 1000) of
    {ok, KVPS, Cont} -> 
      AccOut = fold_over(Fun, AccIn, KVPS),
      fold(Fun, Table, AccOut, Cont);
    {error, Reason} -> {error, Reason}
  end.
%%====================================================================
%% Internal functions
%%====================================================================

fold(Fun, Table, AccIn, Cont) ->
  case osmos:select_continue(Table, Cont, 1000) of
    {ok, [], _} -> AccIn;
    {ok, KVPS, Cont} ->
      AccOut = fold_over(Fun, AccIn, KVPS),
      fold(Fun, Table, AccOut, Cont);
    {error, Reason} -> {error, Reason}
  end.

fold_over(Fun, AccIn, KVPS) ->
  lists:foldl(fun({Key, {Context, Values}}, Acc) ->
      apply(Fun, [{Key, Context, Values}, Acc])
    end, AccIn, KVPS).

osmos_merge(Key, {Context1, Values1}, {Context2, Values2}) ->
  vector_clock:resolve({Context1, Values1}, {Context2, Values2}).
  
osmos_short_circuit(Key, Value) ->
  false.
  
osmos_delete(_, deleted) -> true;
osmos_delete(Key, Value) ->
  false.

osmos_keyless(KeyA, KeyB) when KeyA < KeyB -> true;
osmos_keyless(_, _) -> false.
