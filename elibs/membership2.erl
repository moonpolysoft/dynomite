%%%-------------------------------------------------------------------
%%% File:      membership2.erl
%%% @author    Cliff Moon <cliff@powerset.com> []
%%% @copyright 2009 Cliff Moon
%%% @doc  
%%%
%%% @end  
%%%
%%% @since 2009-05-04 by Cliff Moon
%%%-------------------------------------------------------------------
-module(membership2).
-author('cliff@powerset.com').

-behaviour(gen_server).

-define(VERSION,2).

%% API
-export([start_link/2, register/2, servers_for_key/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

include("../include/config.hrl").

-record(state, {header=?VERSION, node, nodes, partitions, version, servers}).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% @spec start_link() -> {ok,Pid} | ignore | {error,Error}
%% @doc Starts the server
%% @end 
%%--------------------------------------------------------------------
start_link(Node, Nodes) ->
  gen_server:start_link({local, membership}, ?MODULE, [Node, Nodes], []).

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
init([Node, Nodes]) ->
  Config = configuration:get_config(),
  PersistentNodes = load(Node),
  PartialNodes = lists:usort(Nodes ++ PersistentNodes),
  Partners = replication:partners(Node, Nodes, Config),
  {Version, WorldNodes} = join_to(Partners),
  PMap = partitions:create_partitions(Config#config.q, Node, WorldNodes),
  Servers = ets:new(member_servers, [public, set, duplicate_bag]),
  State = #state{node=Node, nodes=Nodes, partitions=PMap, version=Version, servers=Servers},
  save(State),
  {ok, State}.

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
handle_call({join, OtherNode}, _From, State = #state{node=Node, nodes=Nodes}) ->
  Config = configuration:get_config(),
  WorldNodes = lists:usort(Nodes ++ [OtherNode]),
  PMap = partition:create_partitions(Config#config.q, Node, WorldNodes),
  {reply, Reply, State#state{nodes=WorldNodes, partitions=PMap}};

handle_call({servers_for_key, Key}, _From, State = #state{servers=Servers}) ->
  Config = configuration:get_config(),
  Hash = lib_misc:hash(Key),
  Partition = hash_to_partition(Hash, Config#config.q),
  {_, Servers} = lists:unzip(ets:lookup(Servers, Hash)),
  {reply, Servers, State}.

%%--------------------------------------------------------------------
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% @doc Handling cast messages
%% @end 
%%--------------------------------------------------------------------
handle_cast({register, Partition, Pid}, State = #state{servers=Servers}) ->
  Ref = erlang:monitor(process, Pid),
  ets:insert(Servers, {Partition, Pid}),
  ets:insert(Servers, {Ref, Partition, Pid}),
  {noreply, State}.

%%--------------------------------------------------------------------
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% @doc Handling all non call/cast messages
%% @end 
%%--------------------------------------------------------------------
handle_info({'DOWN', Ref, _, Pid, _}, State = #state{servers=Servers}) ->
  erlang:demonitor(Ref),
  [{Ref, Partition, Pid}] = ets:lookup(Servers, Ref),
  ets:delete(Servers, Ref),
  ets:delete_object(Servers, {Partition, Pid}),
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

%% return list of known nodes from membership file
load(Node) ->
  ok.

%% save the list of known nodes to a file
save(State) ->
  ok.

%% return {Version, Nodes} merged version from our partners
join_to(Partners) ->
  ok.
  
hash_to_partition(Hash, Q) ->
  Size = partitions:partition_range(Q),
  Factor = (Hash div Size),
  Rem = (Hash rem Size),
  if
    Rem > 0 -> Factor * Size + 1;
    true -> ((Factor-1) * Size) + 1
  end.
  
int_partitions_for_node(Node, State, master) ->
  Partitions = State#state.partitions,
  {Matching,_} = lists:partition(fun({N,_}) -> N == Node end, Partitions),
  lists:map(fun({_,P}) -> P end, Matching);

int_partitions_for_node(Node, State, all) ->
  %%_Partitions = State#membership.partitions,
  Config = configuration:get_config(),
  Nodes = replication:partners(Node, State#state.nodes, Config),
  lists:foldl(fun(E, Acc) ->
      lists:merge(Acc, int_partitions_for_node(E, State, master))
    end, [], Nodes).