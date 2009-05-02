%%%-------------------------------------------------------------------
%%% File:      storage_manager.erl
%%% @author    Cliff Moon <> []
%%% @copyright 2009 Cliff Moon
%%% @doc  
%%%
%%% @end  
%%%
%%% @since 2009-02-07 by Cliff Moon
%%%-------------------------------------------------------------------
-module(storage_manager).
-author('cliff@powerset.com').

-behaviour(gen_server).

%% API
-export([start_link/0, load/3, load_no_boot/3, loaded/0, stop/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {partitions=[],parts_for_node=[]}).

-include("common.hrl").
-include("config.hrl").

-ifdef(TEST).
-include("etest/storage_manager_test.erl").
-endif.

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% @spec start_link() -> {ok,Pid} | ignore | {error,Error}
%% @doc Starts the server
%% @end 
%%--------------------------------------------------------------------
start_link() ->
  gen_server:start_link({local, storage_manager}, ?MODULE, [], []).
  
load(Nodes, Partitions, PartsForNode) ->
  gen_server:call(storage_manager, {load, Nodes, Partitions, PartsForNode, true}, infinity).
  
load_no_boot(Nodes, Partitions, PartsForNode) ->
  gen_server:call(storage_manager, {load, Nodes, Partitions, PartsForNode, false}, infinity).
  
loaded() ->
  gen_server:call(storage_manager, loaded).
  
stop() ->
  gen_server:cast(storage_manager, stop).

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
init([]) ->
    {ok, #state{}}.

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
handle_call(loaded, _From, State) ->
  {reply, [Name || {registered_name, Name} <- [erlang:process_info(Pid, registered_name) || Pid <- storage_server_sup:storage_servers()]], State};

handle_call({load, Nodes, Partitions, PartsForNode, Bootstrap}, _From, #state{partitions=OldPartitions,parts_for_node=OldPartsForNode}) ->
  Partitions1 = lists:filter(fun(E) ->
      not lists:member(E, OldPartsForNode)
    end, PartsForNode),
  OldPartitions1 = lists:filter(fun(E) ->
      not lists:member(E, PartsForNode)
    end, OldPartsForNode),
  Config = configuration:get_config(),
  % if
  %   length(OldPartitions) == 0 -> 
  %     reload_storage_servers(OldPartitions1, Partitions1, Partitions, Config);
  %   true ->
  reload_storage_servers(OldPartitions1, Partitions1, OldPartitions, Config, Bootstrap),
  % end,
  {reply, ok, #state{partitions=Partitions,parts_for_node=PartsForNode}}.

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
reload_storage_servers(OldParts, NewParts, Old, Config, Bootstrap) ->
  lists:foreach(fun(E) ->
      Name = list_to_atom(lists:concat([storage_, E])),
      supervisor:terminate_child(storage_server_sup, Name),
      supervisor:delete_child(storage_server_sup, Name)
    end, OldParts),
  lists:foreach(fun(Part) ->
    Name = list_to_atom(lists:concat([storage_, Part])),
    DbKey = lists:concat([Config#config.directory, "/", Part]),
    BlockSize = Config#config.blocksize,
    Size = partitions:partition_range(Config#config.q),
    Min = Part,
    Max = Part+Size,
    Spec = {Name, {storage_server,start_link,[Config#config.storage_mod, DbKey, Name, Min, Max, BlockSize]}, permanent, 1000, worker, [storage_server]},
    Callback = fun() ->
        % ?infoFmt("Starting the server for ~p~n", [Spec]),
        case supervisor:start_child(storage_server_sup, Spec) of
          already_present -> supervisor:restart_child(storage_server_sup, Name);
          _ -> ok
        end
      end,
    case {lists:keysearch(Part, 2, Old), Bootstrap} of
      {{value, {OldNode, _}}, true} ->
        bootstrap:start(DbKey, OldNode, Callback);
      _ -> Callback()
    end
  end, NewParts).