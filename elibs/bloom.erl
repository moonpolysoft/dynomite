%%%-------------------------------------------------------------------
%%% File:      bloom.erl
%%% @author    Cliff Moon <cliff@powerset.com> []
%%% @copyright 2009 Cliff Moon
%%% @doc  
%%%
%%% @end  
%%%
%%% @since 2009-04-18 by Cliff Moon
%%%-------------------------------------------------------------------
-module(bloom).
-author('cliff@powerset.com').

%% API
-export([start/3, put/2, has/2, mem_size/1, key_size/1, stop/1]).

%% COMMANDS
-define(SETUP, $s).
-define(PUT, $p).
-define(HAS, $h).
-define(MEM_SIZE, $m).
-define(KEY_SIZE, $k).

-ifdef(TEST).
-include("etest/bloom_test.erl").
-endif.

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% @spec 
%% @doc
%% @end 
%%--------------------------------------------------------------------

start(Filename, N, E) ->
  case load_driver() of
    ok ->
      P = open_port({spawn, 'bloom_drv'}, [binary]),
      port_command(P, [?SETUP, term_to_binary({Filename, N, E})]),
      {ok, {bloom, P}};
    {error, Err} ->
      Msg = erl_ddll:format_error(Err),
      {error, Msg}
  end.

put({bloom, P}, Key) ->
  port_command(P, [?PUT, Key]).
  
has({bloom, P}, Key) ->
  port_command(P, [?HAS, Key]),
  receive
    {P, {data,Bin}} -> binary_to_term(Bin)
  end.
  
mem_size({bloom, P}) ->
  port_command(P, [?MEM_SIZE]),
  receive
    {P, {data,Bin}} -> binary_to_term(Bin)
  end.
  
key_size({bloom, P}) ->
  port_command(P, [?KEY_SIZE]),
  receive
    {P, {data,Bin}} -> binary_to_term(Bin)
  end.
  
stop({bloom, P}) ->
  unlink(P),
  port_close(P).

%%====================================================================
%% Internal functions
%%====================================================================

load_driver() ->
  Dir = filename:join([filename:dirname(code:which(bloom)), "..", "priv"]),
  erl_ddll:load(Dir, "bloom_drv").
  
  