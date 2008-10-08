%%%-------------------------------------------------------------------
%%% File:      dynomite_sup.erl
%%% @author    Cliff Moon <cliff@powerset.com> []
%%% @copyright 2008 Cliff Moon
%%% @doc  
%%%
%%% @end  
%%%
%%% @since 2008-06-27 by Cliff Moon
%%%-------------------------------------------------------------------
-module(dynomite_sup).
-author('cliff moon').

-behaviour(supervisor).

%% API
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

-include("config.hrl").

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================
%%--------------------------------------------------------------------
%% @spec start_link() -> {ok,Pid} | ignore | {error,Error}
%% @doc Starts the supervisor
%% @end 
%%--------------------------------------------------------------------
start_link(Config) ->
    supervisor:start_link(dynomite_sup, Config).

%%====================================================================
%% Supervisor callbacks
%%====================================================================
%%--------------------------------------------------------------------
%% @spec init(Args) -> {ok,  {SupFlags,  [ChildSpec]}} |
%%                     ignore                          |
%%                     {error, Reason}
%% @doc Whenever a supervisor is started using 
%% supervisor:start_link/[2,3], this function is called by the new process 
%% to find out about restart strategy, maximum restart frequency and child 
%% specifications.
%% @end 
%%--------------------------------------------------------------------
init(Config) ->
    {ok,{{one_for_one,10,1}, [
			{configuration, {configuration,start_link,[Config]}, permanent, 1000, worker, [configuration]},
      {storage_server_sup, {storage_server_sup,start_link,[Config]}, permanent, 10000, supervisor, [storage_server_sup]},
      {sync_server_sup, {sync_server_sup,start_link,[Config]}, permanent, 10000, supervisor, [sync_server_sup]},
      {membership, {membership,start_link,[Config]}, permanent, 1000, worker, [membership]},
      {mediator, {mediator,start_link,[Config]}, permanent, 1000, worker, [mediator]},
      {dynomite_web, {dynomite_web,start,[[{port,8080},{docroot, "web"}]]}, permanent, 1000, worker, [dynomite_web]},
      %{ext_listener, {ext_listener,start_link,[Config]}, permanent, 1000, worker, [ext_listener]}
      {socket_server, {socket_server,start_link,[Config]}, permanent, 1000, worker, [socket_server]}
    ]}}.

%%====================================================================
%% Internal functions
%%====================================================================
