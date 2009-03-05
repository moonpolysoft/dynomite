%%%-------------------------------------------------------------------
%%% File:      ulimit.erl
%%% @author    Cliff Moon <> []
%%% @copyright 2009 Cliff Moon
%%% @doc  
%%%
%%% @end  
%%%
%%% @since 2009-01-28 by Cliff Moon
%%%-------------------------------------------------------------------
-module(ulimit).
-author('cliff@powerset.com').

-define(GET, $g).
-define(SET, $s).

%% API
-export([start/0, stop/1, setulimit/2, getulimit/1]).

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
    ok -> {ok, {ulimit, open_port({spawn, ulimit_drv}, [binary])}};
    {error, Err} ->
      Msg = erl_ddll:format_error(Err),
      {error, Msg}
  end.
  
stop({ulimit, P}) ->
  unlink(P),
  exit(P, die).
  
setulimit({ulimit, P}, N) when is_integer(N) ->
  port_command(P, [?SET, term_to_binary(N)]),
  recv(P).
  
getulimit({ulimit, P}) ->
  port_command(P, [?GET]),
  recv(P).
  
%%====================================================================
%% Internal functions
%%====================================================================


load_driver() ->
  Dir = filename:join([filename:dirname(code:which(?MODULE)), "..", "priv"]),
  erl_ddll:load(Dir, "ulimit_drv").

recv(P) ->
  receive
    {P, {data, Bin}} -> binary_to_term(Bin)
  end.