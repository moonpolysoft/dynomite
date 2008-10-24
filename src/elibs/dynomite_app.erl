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
	Config = case application:get_env(jointo) of
		{ok, NodeName} -> 
		  error_logger:info_msg("attempting to contact ~w~n", [NodeName]),
		  case net_adm:ping(NodeName) of
  			pong -> process_arguments([directory, port], configuration:get_config(NodeName));
  			pang -> {error, io_lib:format("Could not connect to ~p.  Exiting.~n", [NodeName])}
  		end;
		undefined -> process_arguments([r, w, n, q, directory, blocksize, port, storage_mod])
	end,
	Options = process_options([web_port]),
  dynomite_sup:start_link(Config, Options).

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

process_options(OptionNames) ->
  process_options(OptionNames, []).

process_options([], Taken) ->
  lists:reverse(Taken);

process_options([Name|OptionNames], Taken) ->
  case application:get_env(Name) of
    undefined -> process_options(OptionNames, Taken);
    {ok, Val} -> process_options(OptionNames, [{Name, Val}|Taken])
  end.
  
process_arguments(Args) ->
	process_arguments(Args, #config{n=3,r=2,w=2,q=6,port=11222,blocksize=4096,directory="/tmp/dynomite",storage_mod=fs_storage,live=true}).
	
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