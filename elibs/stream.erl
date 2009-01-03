%%%-------------------------------------------------------------------
%%% File:      stream.erl
%%% @author    Cliff Moon <cliff@powerset.com> []
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
-export([send/3, recv/3]).

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

recv(Pid, Ref, Timeout) ->
  receive
    {Pid, Ref, {context, Context}} -> recv(Pid, Ref, Timeout, {Context, []})
  after 
    Timeout -> {error, timeout}
  end.
  
recv(Pid, Ref, Timeout, {Context, Values}) ->
  receive
    {Pid, Ref, start_value} -> 
      case recv_value(Pid, Ref, Timeout, <<0:0>>) of
        {error, timeout} -> {error, timeout};
        Value -> recv(Pid, Ref, Timeout, {Context, [Value|Values]})
      end;
    {Pid, Ref, eof} ->
      {ok, {Context, lists:reverse(Values)}}
  after
    Timeout -> {error, timeout}
  end.
  
recv_value(Pid, Ref, Timeout, Bin) ->
  receive
    {Pid, Ref, {data, Data}} -> recv_value(Pid, Ref, Timeout, <<Bin/binary, Data/binary>>);
    {Pid, Ref, end_value} -> Bin
  after 
    Timeout -> {error, timeout}
  end.

send(RemotePid, Ref, {Context, Values}) ->
  RemotePid ! {self(), Ref, {context, Context}},
  send(RemotePid, Ref, Values);
  
send(RemotePid, Ref, []) ->
  RemotePid ! {self(), Ref, eof};
  
send(RemotePid, Ref, [Val|Values]) ->
  RemotePid ! {self(), Ref, start_value},
  send_value(RemotePid, Ref, Val, 0),
  send(RemotePid, Ref, Values).
  
send_value(RemotePid, Ref, Bin, Skip) when Skip >= byte_size(Bin) ->
  RemotePid ! {self(), Ref, end_value};
  
send_value(RemotePid, Ref, Bin, Skip) ->
  if
    (Skip + ?CHUNK_SIZE) > byte_size(Bin) ->
      <<_:Skip/binary, Chunk/binary>> = Bin;
    true ->
      <<_:Skip/binary, Chunk:?CHUNK_SIZE/binary, _/binary>> = Bin
  end,
  RemotePid ! {self(), Ref, {data, Chunk}},
  send_value(RemotePid, Ref, Bin, Skip + ?CHUNK_SIZE).

%%====================================================================
%% Internal functions
%%====================================================================

