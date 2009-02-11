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
-author('cliff@powerset.com').

-behaviour(supervisor).

%% API
-export([start_link/1, start_link/2]).

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
    supervisor:start_link(?MODULE, [Config, []]).

start_link(Config, Options) ->
    supervisor:start_link(?MODULE, [Config, Options]).

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
init([Config, Options]) ->
    Node = node(),
    Nodes = nodes([this,visible]),
    Required = [
                {configuration, 
                 {configuration, start_link, [Config]}, 
                 permanent, 1000, worker, 
                 [configuration]},
                {stats_server, 
                 {stats_server, start_link, []}, 
                 permanent, 1000, worker, 
                 [stats_server]},
                {storage_manager,
                  {storage_manager,start_link, []},
                  permanent, 1000, worker, [storage_manager]},
                {storage_server_sup, 
                 {storage_server_sup, start_link, [Config]}, 
                 permanent, 10000, supervisor, 
                 [storage_server_sup]},
                {sync_manager, 
                 {sync_manager, start_link, []}, 
                 permanent, 1000, worker, [sync_manager]},
                {sync_server_sup,
                 {sync_server_sup, start_link, [Config]}, 
                 permanent, 10000, supervisor, 
                 [sync_server_sup]},
                {membership, 
                 {membership, start_link, [Node, Nodes]}, 
                 permanent, 1000, worker, 
                 [membership]},
                {mediator, 
                 {mediator, start_link, [Config]}, 
                 permanent, 1000, worker, 
                 [mediator]},
                %% FIXME these should be optional as well?
                {socket_server, 
                 {socket_server, start_link, [Config]}, 
                 permanent, 1000, worker, 
                 [socket_server]},
                {dynomite_thrift_service, 
                 {dynomite_thrift_service, start_link, [Config]}, 
                 permanent, 1000, worker, 
                 [dynomite_thrift_service]}
               ],
    Optional = 
        case Options of
            [] ->
                [];
            _ ->
                [{dynomite_web, 
                  {dynomite_web, start, [Options]}, 
                  permanent, 1000, worker, 
                  [dynomite_web]}]
        end,    
    Children = Required ++ Optional,    
    {ok,{{one_for_one,10,1}, Children}}.

%%====================================================================
%% Internal functions
%%====================================================================
