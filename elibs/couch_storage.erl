%%%-------------------------------------------------------------------
%%% File:      couch_storage.erl
%%% @author    Cliff Moon <cliff@powerset.com> []
%%% @copyright 2008 Cliff Moon
%%% @doc  
%%%
%%% @end  
%%%
%%% @since 2008-11-11 by Cliff Moon
%%%-------------------------------------------------------------------
-module(couch_storage).
-author('cliff@powerset.com').

%% API
-export([open/2, close/1, get/2, put/4, has_key/2, delete/2, fold/3]).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% @spec 
%% @doc
%% @end 
%%--------------------------------------------------------------------
open(Directory, Name) ->
  FileName = lists:concat([Directory, "/", Name]),
  {ok, Fd} = case filelib:is_file(FileName) of
    true -> couch_file:open(FileName, []);
    false -> couch_file:open(FileName, [create])
  end,
  couch_btree:open(nil, Fd).
  
close(Db) ->
  couch_file:close(element(2, Db)).
  
fold(Fun, Db, AccIn) ->
  {ok, AccOut} = couch_btree:foldl(Db, fun({Key, {Context, Values}}, Acc) ->
      % error_logger:info_msg("folding: ~p ~p ~p ~n", [Key, {Context,Values}, Acc]),
      Next = Fun({Key, Context, Values}, Acc),
      {ok, Next}
    end, AccIn),
  AccOut.
  
put(Key, Context, Values, Db) ->
  couch_btree:add(Db, [{Key, {Context,Values}}]).
  
get(Key, Db) ->
  case couch_btree:lookup(Db, [Key]) of
    [not_found] -> {ok, not_found};
    [{ok, {Key, {Context,Values}}}] -> {ok, {Context,Values}}
  end.
  
%% no equiv, i'm afraid
has_key(Key ,Db) ->
  case couch_btree:lookup(Db, [Key]) of
    [not_found] -> {ok, false};
    [{ok, _}] -> {ok, true}
  end.
  
delete(Key, Db) ->
  case couch_btree:add_remove(Db, [], [Key]) of
    {ok, [], ModDb} -> {ok, ModDb};
    Failure -> Failure
  end.

%%====================================================================
%% Internal functions
%%====================================================================

