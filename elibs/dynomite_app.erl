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
-author('cliff moon').

-behaviour(application).

-include("config.hrl").

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
	Config = case application:get_env(join) of
		{ok, NodeName} -> case net_adm:ping(NodeName) of
			pong -> process_arguments([directory, port], configuration:get_config(NodeName));
			pang -> {error, io_lib:format("Could not connect to ~w.  Exiting.~n", [NodeName])}
		end;
		undefined -> process_arguments([r, w, n, q, directory, port])
	end,
  dynomite_sup:start_link(Config).

%%--------------------------------------------------------------------
%% @spec stop(State) -> void()
%% @doc This function is called whenever an application
%% has stopped. It is intended to be the opposite of Module:start/2 and
%% should do any necessary cleaning up. The return value is ignored. 
%% @end 
%%--------------------------------------------------------------------
stop({_, Sup}) ->
  exit(Sup, shutdown),
  ok.

%%====================================================================
%% Internal functions
%%====================================================================

process_arguments(Args) ->
	process_arguments(Args, #config{n=3,r=2,w=2,q=10,directory="/tmp/dynomite"}).
	
process_arguments([], Config) -> Config;

process_arguments([Arg | ArgList], Config) ->
	process_arguments(ArgList, 
		case application:get_env(Arg) of
			{ok, Val} -> config_replace(Arg, Config, Val);
			undefined -> Config
		end).
		
config_replace(Field, Tuple, Value) ->
  config_replace(record_info(fields, config), Field, Tuple, Value, 2).
  
config_replace([Field | _], Field, Tuple, Value, Index) ->
  setelement(Index, Tuple, Value);
  
config_replace([_|Fields], Field, Tuple, Value, Index) ->
  config_replace(Fields, Field, Tuple, Value, Index+1).