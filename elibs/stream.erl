%%%-------------------------------------------------------------------
%%% File:      stream.erl
%%% @author    Cliff Moon <> []
%%% @copyright 2009 Cliff Moon
%%% @doc  
%%%
%%% @end  
%%%
%%% @since 2009-01-01 by Cliff Moon
%%%-------------------------------------------------------------------
-module(stream).
-author('cliff@powerset.com').

-include("chunk_size.hrl").

%% API
-export([reply/2, recv/2]).

-ifdef(TEST).
-include("etest/stream_test.erl").
-endif.

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% @spec 
%% @doc
%% @end 
%%--------------------------------------------------------------------

recv(Pid, Timeout) ->
  receive
    {Pid, {context, Context}} -> recv(Pid, Timeout, {Context, []})
  after 
    Timeout -> {error, timeout}
  end.
  
recv(Pid, Timeout, {Context, Values}) ->
  receive
    {Pid, start_value} -> 
      case recv_value(Pid, Timeout, <<0:0>>) of
        {error, timeout} -> {error, timeout};
        Value -> recv(Pid, Timeout, {Context, [Value|Values]})
      end;
    {Pid, eof} ->
      {ok, {Context, lists:reverse(Values)}}
  after
    Timeout -> {error, timeout}
  end.
  
recv_value(Pid, Timeout, Bin) ->
  receive
    {Pid, {data, Data}} -> recv_value(Pid, Timeout, <<Bin/binary, Data/binary>>);
    {Pid, end_value} -> Bin
  after 
    Timeout -> {error, timeout}
  end.

reply(RemotePid, {Context, Values}) ->
  RemotePid ! {self(), {context, Context}},
  reply(RemotePid, Values);
  
reply(RemotePid, []) ->
  RemotePid ! {self(), eof};
  
reply(RemotePid, [Val|Values]) ->
  RemotePid ! {self(), start_value},
  stream_value(RemotePid, Val, 0),
  reply(RemotePid, Values).
  
stream_value(RemotePid, Bin, Skip) when Skip >= byte_size(Bin) ->
  RemotePid ! {self(), end_value};
  
stream_value(RemotePid, Bin, Skip) ->
  if
    (Skip + ?CHUNK_SIZE) > byte_size(Bin) ->
      <<_:Skip/binary, Chunk/binary>> = Bin;
    true ->
      <<_:Skip/binary, Chunk:?CHUNK_SIZE/binary, _/binary>> = Bin
  end,
  RemotePid ! {self(), {data, Chunk}},
  stream_value(RemotePid, Bin, Skip + ?CHUNK_SIZE).

%%====================================================================
%% Internal functions
%%====================================================================

