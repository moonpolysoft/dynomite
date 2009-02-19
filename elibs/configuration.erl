%%%-------------------------------------------------------------------
%%% File:      configuration.erl
%%% @author    Cliff Moon <> []
%%% @copyright 2008 Cliff Moon
%%% @doc  
%%%
%%% @end  
%%%
%%% @since 2008-07-18 by Cliff Moon
%%%-------------------------------------------------------------------
-module(configuration).
-author('cliff@powerset.com').

-behaviour(gen_server).

%% API
-export([start_link/1, get_config/1, get_config/0, set_config/1, stop/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("config.hrl").
-include("common.hrl").

-ifdef(TEST).
-include("etest/configuration_test.erl").
-endif.

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% @spec start_link() -> {ok,Pid} | ignore | {error,Error}
%% @doc Starts the server
%% @end 
%%--------------------------------------------------------------------
start_link(ConfigFile) ->
  gen_server:start_link({local, configuration}, configuration, ConfigFile, []).

get_config(Node) ->
  gen_server:call({configuration, Node}, get_config, 1000).

get_config() ->
  case get(config) of
    undefined -> 
      C = gen_server:call(configuration, get_config),
      put(config, C),
      C;
    C -> C
  end.
  
set_config(Config) ->
  gen_server:call(configuration, {set_config, Config}).

stop() ->
    gen_server:cast(configuration, stop).


%%====================================================================
%% gen_server callbacks
%%====================================================================

%%--------------------------------------------------------------------
%% @spec init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% @doc Initiates the server
%% @end 
%%--------------------------------------------------------------------
init(Config = #config{}) ->
  {ok, Config};

init(ConfigFile) when is_list(ConfigFile) ->
  case read_config_file(ConfigFile) of
    {ok, Config} -> 
      filelib:ensure_dir(Config#config.directory ++ "/"),
      {ok, Config};
    {error, Reason} -> {error, Reason}
  end.

%%--------------------------------------------------------------------
%% @spec 
%% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% @doc Handling call messages
%% @end 
%%--------------------------------------------------------------------

handle_call(get_config, _From, State) ->
	{reply, State, State};
	
handle_call({set_config, Config}, _From, State) ->
  {reply, ok, Config}.

%%--------------------------------------------------------------------
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% @doc Handling cast messages
%% @end 
%%--------------------------------------------------------------------
handle_cast(stop, State) ->
    {stop, shutdown, State}.

%%--------------------------------------------------------------------
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% @doc Handling all non call/cast messages
%% @end 
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @spec terminate(Reason, State) -> void()
%% @doc This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%% @end 
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @doc Convert process state when code is changed
%% @end 
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------
read_config_file(ConfigFile) ->
  case file:read_file(ConfigFile) of
    {ok, Bin} -> {ok, decode_json(mochijson:decode(Bin))};
    {error, Reason} -> {error, Reason}
  end.
  
decode_json({struct, Options}) ->
  decode_json(Options, #config{}).
  
decode_json([], Config) ->
  Config;
  
decode_json([{Field,null} | Options], Config) -> % null is undefined lol
  decode_json(Options, config_replace(list_to_atom(Field), Config, undefined));
  
decode_json([{Field,Value} | Options], Config) ->
  decode_json(Options, config_replace(list_to_atom(Field), Config, Value)).
  
config_replace(Field, Tuple, Value) ->
  config_replace(record_info(fields, config), Field, Tuple, Value, 2).

config_replace([], Field, Tuple, _, _) ->
  Tuple;

config_replace([Field | _], Field, Tuple, Value, Index) ->
  setelement(Index, Tuple, Value);

config_replace([_|Fields], Field, Tuple, Value, Index) ->
  config_replace(Fields, Field, Tuple, Value, Index+1).