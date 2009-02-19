%%%-------------------------------------------------------------------
%%% File:      untitled.erl
%%% @author    Cliff Moon <cliff@powerset.com> []
%%% @copyright 2008 Cliff Moon
%%% @doc  
%%%
%%% @end  
%%%
%%% @since 2008-06-27 by Cliff Moon
%%%-------------------------------------------------------------------
-module(storage_server_sup).
-author('cliff@powerset.com').

-behaviour(supervisor).

%% API
-export([start_link/0, storage_servers/0]).

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
start_link() ->
  supervisor:start_link({local, storage_server_sup}, storage_server_sup, []).
    
storage_servers() ->
  lists:filter(fun
      (undefined) -> false;
      (Child) -> true
    end, lists:map(fun({_Id, Child, _Type, _Modules}) -> 
        Child
      end, supervisor:which_children(storage_server_sup))).

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
init([]) ->
  ChildSpecs = [],
  {ok,{{one_for_one,10,1}, ChildSpecs}}.

%%====================================================================
%% Internal functions
%%====================================================================
