%%%-------------------------------------------------------------------
%%% File:      fnv.erl
%%% @author    Cliff Moon <> []
%%% @copyright 2009 Cliff Moon
%%% @doc  
%%%
%%% @end  
%%%
%%% @since 2009-01-29 by Cliff Moon
%%%-------------------------------------------------------------------
-module(fnv).
-author('cliff@powerset.com').

-include("common.hrl").

-define(SEED, 2166136261).
%% API
-export([start/0, stop/1, hash/1, hash/2]).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% @spec 
%% @doc
%% @end 
%%--------------------------------------------------------------------
start() ->
  case load_driver() of
    ok ->
      Pid = spawn_link(fun() ->
          P = open(),
          register(fnv_drv, P),
          loop(P)
        end),
      timer:sleep(1),
      {ok, Pid};
    {error, Err} ->
      Msg = erl_ddll:format_error(Err),
      {error, Msg}
  end.

stop(P) ->
  unlink(P),
  exit(P, die).
  
hash(Thing) ->
  hash(Thing, ?SEED).
  
% hash(Thing, Seed) when is_list(Thing) -> %assume io_list
%   P = get_or_open(),
%   port_command(P, [term_to_binary(Seed)] ++ Thing),
%   recv(P);
  
hash(Thing, Seed) when is_binary(Thing) ->
  P = get_or_open(),
  convert(port_control(P, Seed, Thing));
  % recv(P);
  
hash(Thing, Seed) ->
  P = get_or_open(),
  convert(port_control(P, Seed, term_to_binary(Thing))).
  % recv(P).

%%====================================================================
%% Internal functions
%%====================================================================
loop(P) ->
  receive _ -> loop(P) end.

convert(List) ->
  <<Hash:32/unsigned-integer>> = list_to_binary(List),
  Hash.

get_or_open() ->
  case get(fnv_drv) of
    undefined ->
      load_driver(),
      P = open(),
      put(fnv_drv, P),
      P;
    P -> P
  end.

open() ->
  open_port({spawn, fnv_drv}, [binary]).

load_driver() ->
  Dir = filename:join([filename:dirname(code:which(?MODULE)), "..", "priv"]),
  erl_ddll:load(Dir, "fnv_drv").

recv(P) ->
  receive
    {P, {data, Bin}} -> binary_to_term(Bin);
    V -> V
  end.
  

