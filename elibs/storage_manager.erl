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
-export([start_link/0, load/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {partitions,specs}).

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
  
load(Node, Nodes, Partitions) ->
  gen_server:call(storage_manager, {load, Node, Nodes, Partitions}).

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
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% @doc Handling cast messages
%% @end 
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

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
% reload_storage_servers(empty, NewState) ->
%   Config = configuration:get_config(),
%   Partitions = int_partitions_for_node(node(), NewState, all),
%   Old = NewState#membership.old_partitions,
%   reload_storage_servers([], Partitions, Old, Config);
% 
% reload_storage_servers(OldState, NewState) ->
%   Config = configuration:get_config(),
%   PartForNode = int_partitions_for_node(node(), NewState, all),
%   OldPartForNode = int_partitions_for_node(node(), OldState, all),
%   Old = OldState#membership.partitions,
%   NewPartitions = lists:filter(fun(E) ->
%       not lists:member(E, OldPartForNode)
%     end, PartForNode),
%   OldPartitions = lists:filter(fun(E) ->
%       not lists:member(E, PartForNode)
%     end, OldPartForNode),
%   reload_storage_servers(OldPartitions, NewPartitions, Old, Config).
%   
% reload_storage_servers(_, _, _, #config{live=Live}) when not Live ->
%   ok;
%   
% reload_storage_servers(OldParts, NewParts, Old, Config = #config{live=Live}) when Live ->
%   lists:foreach(fun(E) ->
%       Name = list_to_atom(lists:concat([storage_, E])),
%       supervisor:terminate_child(storage_server_sup, Name),
%       supervisor:delete_child(storage_server_sup, Name)
%     end, OldParts),
%   lists:foreach(fun(Part) ->
%     Name = list_to_atom(lists:concat([storage_, Part])),
%     DbKey = lists:concat([Config#config.directory, "/", Part]),
%     BlockSize = Config#config.blocksize,
%     {Min,Max} = int_range(Part, Config),
%     Spec = {Name, {storage_server,start_link,[Config#config.storage_mod, DbKey, Name, Min, Max, BlockSize]}, permanent, 1000, worker, [storage_server]},
%     Callback = fun() ->
%         ?infoFmt("Starting the server for ~p~n", [Spec]),
%         case supervisor:start_child(storage_server_sup, Spec) of
%           already_present -> supervisor:restart_child(storage_server_sup, Name);
%           _ -> ok
%         end
%       end,
%     case catch lists:keysearch(Part, 2, Old) of
%       {value, {OldNode, _}} -> bootstrap:start(DbKey, OldNode, Callback);
%       _ -> Callback()
%     end
%   end, NewParts).