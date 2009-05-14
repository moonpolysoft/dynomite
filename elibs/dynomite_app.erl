%%%-------------------------------------------------------------------
%%% File:      dynomite.erl
%%% @author    Cliff Moon <cliff@powerset.com> []
%%% @copyright 2008 Cliff Moon
%%% @doc
%%%
%%% @end
%%%
%%% @since 2008-06-27 by Cliff Moon
%%%-------------------------------------------------------------------
-module(dynomite_app).
-author('cliff@powerset.com').

-behaviour(application).

-include("../include/config.hrl").
-include("../include/common.hrl").

%% Application callbacks
-export([start/2, stop/1]).

%%====================================================================
%% Application callbacks
%%====================================================================
%%--------------------------------------------------------------------
%% @spec start(Type, StartArgs) -> {ok, Pid} |
%%                                 {ok, Pid, State} |
%%                                 {error, Reason}
%% @doc This function is called whenever an application
%% is started using application:start/1,2, and should start the processes
%% of the application. If the application is structured according to the
%% OTP design principles as a supervision tree, this means starting the
%% top supervisor of the tree.
%% @end
%%--------------------------------------------------------------------
start(_Type, []) ->
  case application:get_env(config) of
    {ok, ConfigFile} ->
      case filelib:is_file(ConfigFile) of
        true -> join_and_start(ConfigFile);
        false -> {error, ?fmt("~p does not exist.", [ConfigFile])}
      end;
    undefined ->
      {error, ?fmt("No config file given.", [])}
  end.

%%--------------------------------------------------------------------
%% @spec stop(State) -> void()
%% @doc This function is called whenever an application
%% has stopped. It is intended to be the opposite of Module:start/2 and
%% should do any necessary cleaning up. The return value is ignored.
%% @end
%%--------------------------------------------------------------------
stop({_, Sup}) ->
  exit(Sup, normal),
  ok.

%%====================================================================
%% Internal functions
%%====================================================================
verify_ulimit(#config{q=Q}) ->
  Partitions = (2 bsl (Q-1)),
  % this is our estimated max # of fd's 2 per partition and 100 for connections
  FD = Partitions * 3 + 103,
  case ulimit:start() of
    {ok, U} -> alter_ulimit(U, FD);
    {error, Msg} -> error_logger:error_msg("Could not load ulimit driver ~p~n", [Msg])
  end.

alter_ulimit(U, FD) ->
  case ulimit:getulimit(U) of
    {SoftLim, _} when SoftLim < FD ->
      error_logger:info_msg("Setting ulimit to ~p to match the partition map.~n", [FD]),
      ulimit:setulimit(U, FD);
    _ -> ok
  end,
  ulimit:stop(U).

join_and_start(ConfigFile) ->
  case application:get_env(jointo) of
    {ok, NodeName} ->
      ?infoFmt("attempting to contact ~p~n", [NodeName]),
      case net_adm:ping(NodeName) of
        pong ->
          ?infoFmt("Connected to ~p~n", [NodeName]),
          dynomite_sup:start_link(ConfigFile);
        pang ->
          {error, ?fmt("Could not connect to ~p.  Exiting.", [NodeName])}
      end;
    undefined -> dynomite_sup:start_link(ConfigFile)
  end.
