%%%-------------------------------------------------------------------
%%% File:      untitled.erl
%%% @author    Cliff Moon <cliff@powerset.com> []
%%% @copyright 2008 Cliff Moon
%%% @doc  Membership process keeps track of dynomite node membership.  Maintains a version history.
%%%
%%% @end  
%%%
%%% @since 2008-03-30 by Cliff Moon
%%%-------------------------------------------------------------------
-module(membership).
-author('cliff@powerset.com').

-behaviour(gen_server).

%% API
-export([start_link/3, start_link/2, join_node/2, nodes_for_partition/1, replica_nodes/1, servers_for_key/1, nodes_for_key/1, partitions/0, nodes/0, state/0, partitions_for_node/2, fire_gossip/1, partition_for_key/1, stop/0, stop/1, range/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(VERSION, 1).

-record(membership, {header=?VERSION, partitions, version, nodes, node, gossip}).

-include("config.hrl").
-include("common.hrl").

-ifdef(TEST).
-include("etest/membership_test.erl").
-endif.

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% @spec start_link() -> {ok,Pid} | ignore | {error,Error}
%% @doc Starts the server
%% @end 
%%--------------------------------------------------------------------
start_link(Name, Node, Nodes) ->
  gen_server:start_link({local, Name}, ?MODULE, [Node, Nodes], []).

start_link(Node, Nodes) ->
  gen_server:start_link({local, membership}, ?MODULE, [Node, Nodes], []).

join_node(JoinTo, Me) ->
  (catch gen_server:call({membership, JoinTo}, {join_node, Me})).
	
servers_for_key(Key) ->
  gen_server:call(membership, {servers_for_key, Key}).
	
nodes_for_partition(Partition) ->
  gen_server:call(membership, {nodes_for_partition, Partition}).
	
nodes_for_key(Key) ->
  gen_server:call(membership, {nodes_for_key, Key}).
  
nodes() ->
  gen_server:call(membership, nodes).
  
state() ->
  gen_server:call(membership, state).
  
replica_nodes(Node) ->
  gen_server:call(membership, {replica_nodes, Node}).
  
partitions() ->
  gen_server:call(membership, partitions).
  
partitions_for_node(Node, Option) ->
  gen_server:call(membership, {partitions_for_node, Node, Option}).
  
partition_for_key(Key) ->
  gen_server:call(membership, {partition_for_key, Key}).

range(Partition) ->
  gen_server:call(membership, {range, Partition}).

stop() ->
  gen_server:cast(membership, stop).
  
stop(Server) ->
  gen_server:cast(Server, stop).

fire_gossip(Node) when is_atom(Node) ->
  ?infoFmt("firing gossip at ~p~n", [Node]),
	gen_server:call(membership, {gossip_with, {membership, Node}}).

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
  process_flag(trap_exit, true), % this is for the gossip server which tags along
  Config = configuration:get_config(),
  State = try_join_into_cluster(Node, create_or_load_state(Node, Nodes, Config)),
  ?infoMsg("Saving membership data.~n"),
  save_state(State),
  ?infoMsg("Loading storage servers.~n"),
  storage_manager:load(Node, Nodes, int_partitions_for_node(Node, State, all)),
  ?infoMsg("Loading sync servers.~n"),
  sync_manager:load(Node, Nodes, int_partitions_for_node(Node, State, all)),
  ?infoMsg("Starting membership gossip.~n"),
  Self = self(),
  GossipPid = spawn_link(fun() -> gossip_loop(Self) end),
  % timer:apply_after(random:uniform(1000) + 1000, membership, fire_gossip, [random:seed()]),
  ?infoMsg("Initialized.~n"),
  {ok, State#membership{gossip=GossipPid}}.

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

handle_call({join_node, Node}, {_, _From}, State) ->
  error_logger:info_msg("~p is joining the cluster.~n", [node(_From)]),
  NewState = int_join_node(Node, State),
  #membership{node=Node1,nodes=Nodes,partitions=Parts} = NewState,
  storage_manager:load(Nodes, Parts, int_partitions_for_node(Node1, NewState, all)),
  sync_manager:load(Nodes, Parts, int_partitions_for_node(Node1, NewState, all)),
  save_state(NewState),
	{reply, {ok, NewState}, NewState};

handle_call({gossip_with, Server}, From, State = #membership{nodes = Nodes}) ->
  % error_logger:info_msg("firing gossip at ~p~n", [Node]),

  % share our state with target node - expects a response back, but we don't
  % want to wait for it
  Self = self(),
  GosFun =
    fun() ->
        {ok, RemoteState} = gen_server:call(Server, {share, State}),
        {ok, ModState} = gen_server:call(Self, {share, RemoteState}),
        gen_server:reply(From, {ok, ModState})
    end,
  spawn_link(GosFun),
  {noreply, State};
%%
% Another node is sharing their state with us. Need to merge them in
% and reply with the merged state
%%
handle_call({share, RemoteState}, _From, State) ->
  {ok, Merged} = merge_and_save_state(RemoteState, State),
  {reply, {ok, Merged}, Merged};
	
handle_call(nodes, _From, State = #membership{nodes=Nodes}) ->
  {reply, Nodes, State};
  
handle_call(state, _From, State) -> {reply, State, State};
	
handle_call(partitions, _From, State) -> {reply, State#membership.partitions, State};
	
handle_call({replica_nodes, Node}, _From, State) ->
  {reply, int_replica_nodes(Node, State), State};
	
handle_call({range, Partition}, _From, State) ->
  {reply, int_range(Partition, configuration:get_config()), State};
	
handle_call({nodes_for_partition, Partition}, _From, State) ->
  {reply, int_nodes_for_partition(Partition, State), State};
	
handle_call({servers_for_key, Key}, _From, State) ->
  Nodes = int_nodes_for_key(Key, State),
  Part = int_partition_for_key(Key, State),
  MapFun = fun(Node) -> {list_to_atom(lists:concat([storage_, Part])), Node} end,
  {reply, lists:map(MapFun, Nodes), State};
	
handle_call({nodes_for_key, Key}, _From, State) ->
	{reply, int_nodes_for_key(Key, State), State};
	
handle_call({partitions_for_node, Node, Option}, _From, State) ->
  {reply, int_partitions_for_node(Node, State, Option), State};
  
handle_call({partition_for_key, Key}, _From, State) ->
  {reply, int_partition_for_key(Key, State), State};
	
handle_call(stop, _From, State) ->
  {stop, shutdown, ok, State}.

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

gossip_loop(Server) ->
  #membership{nodes=Nodes,node=Node} = gen_server:call(Server, state),
  case lists:delete(Node, Nodes) of
    [] -> ok; % no other nodes
    Nodes1 when is_list(Nodes1) ->
      ?infoFmt("nodes1 ~p", [Nodes1]),
      fire_gossip(random_node(Nodes1))
  end,
  receive
    stop -> gossip_paused(Server);
    _Val -> ok
  end,
  timer:sleep(random:uniform(5000) + 5000),
  gossip_loop(Server).
  
gossip_paused(Server) ->
  receive
    start -> ok
  end.

int_range(Partition, #config{q=Q}) ->
  Size = partitions:partition_range(Q),
  {Partition, Partition+Size}.

random_node(Nodes) -> 
  lists:nth(random:uniform(length(Nodes)), Nodes).
  
% random_nodes(N, Nodes) -> random_nodes(N, Nodes, []).
%   
% random_nodes(_, [], Taken) -> Taken;
%   
% random_nodes(0, _, Taken) -> Taken;
%   
% random_nodes(N, Nodes, Taken) ->
%   {One, Two} = lists:split(random:uniform(length(Nodes)), Nodes),
%   if
%     length(Two) > 0 -> 
%       [Head|Split] = Two,
%       random_nodes(N-1, One ++ Split, [Head|Taken]);
%     true ->
%       [Head|Split] = One,
%       random_nodes(N-1, Split, [Head|Taken])
%   end.

% we are alone in the world
try_join_into_cluster(Node, State = #membership{nodes=[Node]}) ->
  State;
  
try_join_into_cluster(Node, State = #membership{nodes=Nodes}) ->
  JoinTo = random_node(lists:delete(Node, Nodes)),
  error_logger:info_msg("Joining node ~p~n", [JoinTo]),
  case join_node(JoinTo, Node) of
    {ok, JoinedState} -> JoinedState#membership{node=Node};
    Other ->
      error_logger:info_msg("Join to ~p failed with ~p~n", [JoinTo, Other]),
      State
  end.

create_or_load_state(Node, Nodes, Config) ->
  case load_state(Node, Config) of
    {ok, Value = #membership{header=?VERSION,nodes=LoadedNodes}} ->
      error_logger:info_msg("loaded membership from disk~n", []),
      Value#membership{node=Node,nodes=lists:umerge([Nodes, LoadedNodes])};
    _V -> 
      create_initial_state(Node, Nodes, Config)
  end.

load_state(Node, #config{directory=Directory}) ->
  case file:read_file(filename:join(Directory, atom_to_list(Node) ++ ".bin")) of
    {ok, Binary} -> 
      {ok, binary_to_term(Binary)};
    _ -> not_found
  end. 

save_state(State) ->
  Node = State#membership.node,
  Config = configuration:get_config(),
  Binary = term_to_binary(State),
  {ok, File} = file:open(filename:join(Config#config.directory, atom_to_list(Node) ++ ".bin"), [write, raw]),
  ok = file:write(File, Binary),
  ok = file:close(File).

%% partitions is a list starting with 1 which defines a partition space.
create_initial_state(Node, Nodes, Config) ->
  Q = Config#config.q,
  #membership{
    version=vector_clock:create(pid_to_list(self())),
	  partitions=partitions:create_partitions(Q, Node, Nodes),
	  node=Node,
	  nodes=Nodes}.

merge_states(StateA, StateB) ->
  PartA = StateA#membership.partitions,
  PartB = StateB#membership.partitions,
  Config = configuration:get_config(),
  Nodes = lists:usort(StateA#membership.nodes ++ StateB#membership.nodes),
  Partitions = partitions:map_partitions(PartA, Nodes),
  % error_logger:info_msg("Merged nodes ~p and partitions ~p~n", [Nodes, length(Partitions)]),
  ?infoFmt("merge states setting node to ~p", [StateA#membership.node]),
  #membership{
    version=vector_clock:merge(StateA#membership.version, StateB#membership.version),
    nodes=Nodes,
    node=StateA#membership.node,
    partitions=Partitions,
    gossip=StateA#membership.gossip
  }.

% Merges in another state, reloads any storage
% and sync servers that changed, saves state
% to disk.
%
% merge_and_save_state(state(), state()) -> {ok, NewState :: state()}
merge_and_save_state(RemoteState, State) ->
  Merged =
    case vector_clock:compare(State#membership.version, RemoteState#membership.version) of
      less -> % remote state is strictly newer than ours
        RemoteState#membership{node=State#membership.node,gossip=State#membership.gossip};
      greater -> % remote state is strictly older
        State;
      equal -> % same vector clock
        State;
      concurrent -> % must merge
        merge_states(RemoteState, State)
  end,
  #membership{node=Node,nodes=Nodes,partitions=Parts} = Merged,
  storage_manager:load(Node, Nodes, int_partitions_for_node(Node, Merged, all)),
  sync_manager:load(Node, Nodes, int_partitions_for_node(Node, Merged, all)),
  save_state(Merged),
  {ok, Merged}.

int_join_node(NewNode, #membership{node=Node,partitions=Partitions,version=Version,nodes=OldNodes,gossip=Gossip}) ->
  Nodes = lists:usort([NewNode|OldNodes]),
  P = partitions:map_partitions(Partitions, Nodes),
  ?infoFmt("int join setting node to ~p", [Node]),
  #membership{
    partitions=P,
    version = vector_clock:increment(pid_to_list(self()), Version),
    node=Node,
    nodes=Nodes,
    gossip=Gossip}.
  
int_partitions_for_node(Node, State, master) ->
  Partitions = State#membership.partitions,
  {Matching,_} = lists:partition(fun({N,_}) -> N == Node end, Partitions),
  lists:map(fun({_,P}) -> P end, Matching);
  
int_partitions_for_node(Node, State, all) ->
  Partitions = State#membership.partitions,
  Nodes = reverse_replica_nodes(Node, State),
  lists:foldl(fun(E, Acc) -> 
      lists:merge(Acc, int_partitions_for_node(E, State, master)) 
    end, [], Nodes).
  
reverse_replica_nodes(Node, State) ->
  Config = configuration:get_config(),
  n_nodes(Node, Config#config.n, lists:reverse(State#membership.nodes)).
  
int_replica_nodes(Node, State) ->
  Config = configuration:get_config(),
  n_nodes(Node, Config#config.n, State#membership.nodes).
  
int_nodes_for_key(Key, State) ->
  % error_logger:info_msg("inside int_nodes_for_key~n", []),
  KeyHash = lib_misc:hash(Key),
  Config = configuration:get_config(),
  Q = Config#config.q,
  Partition = find_partition(KeyHash, Q),
  % error_logger:info_msg("found partition ~w for key ~p~n", [Partition, Key]),
  int_nodes_for_partition(Partition, State).
  
int_nodes_for_partition(Partition, State) ->
  Config = configuration:get_config(),
  Partitions = State#membership.partitions,
  Q = Config#config.q,
  N = Config#config.n,
  {Node,Partition} = lists:nth(index_for_partition(Partition, Q), Partitions),
  % error_logger:info_msg("Node ~w Partition ~w N ~w~n", [Node, Partition, N]),
  int_replica_nodes(Node, State).
  
int_partition_for_key(Key, State) ->
  KeyHash = lib_misc:hash(Key),
  Config = configuration:get_config(),
  Q = Config#config.q,
  find_partition(KeyHash, Q).
  
find_partition(Hash, Q) ->
  Size = partitions:partition_range(Q),
  Factor = (Hash div Size),
  Rem = (Hash rem Size),
  if
    Rem > 0 -> Factor * Size + 1;
    true -> ((Factor-1) * Size) + 1
  end.
  
%1 based index, thx erlang
index_for_partition(Partition, Q) ->
  Size = partitions:partition_range(Q),
  Index = (Partition div Size) + 1.
  
n_nodes(StartNode, N, Nodes) ->
  if
    N >= length(Nodes) -> Nodes;
    true -> n_nodes(StartNode, N, Nodes, [], Nodes)
  end.
  
n_nodes(_, 0, _, Taken, _) -> lists:reverse(Taken);

n_nodes(StartNode, N, [], Taken, Cycle) -> n_nodes(StartNode, N, Cycle, Taken, Cycle);

n_nodes(found, N, [Head|Nodes], Taken, Cycle) ->
  n_nodes(found, N-1, Nodes, [Head|Taken], Cycle);
  
n_nodes(StartNode, N, [StartNode|Nodes], Taken, Cycle) ->
  n_nodes(found, N-1, Nodes, [StartNode|Taken], Cycle);
  
n_nodes(StartNode, N, [_|Nodes], Taken, Cycle) ->
  n_nodes(StartNode, N, Nodes, Taken, Cycle).
  
