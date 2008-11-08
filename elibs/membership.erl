%%%-------------------------------------------------------------------
%%% File:      untitled.erl
%%% @author    Cliff Moon <> []
%%% @copyright 2008 Cliff Moon
%%% @doc  Membership process keeps track of dynomite node membership.  Maintains a version history.
%%%
%%% @end  
%%%
%%% @since 2008-03-30 by Cliff Moon
%%%-------------------------------------------------------------------
-module(membership).
-author('Cliff Moon').

-behaviour(gen_server).
-define(VIRTUALNODES, 100).

%% API
-export([start_link/1, join_node/2, nodes_for_partition/1, replica_nodes/1, nodes_for_key/1, partitions/0, nodes/0, state/0, old_partitions/0, partitions_for_node/2, fire_gossip/1, partition_for_key/1, stop/0, range/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(membership, {config, partitions, version, nodes, old_partitions}).

-include("config.hrl").

-define(power_2(N), (2 bsl (N-1))).

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
start_link(Config) ->
  gen_server:start_link({local, membership}, ?MODULE, Config, []).

join_node(JoinTo, Me) ->
	gen_server:call({membership, JoinTo}, {join_node, Me}).
	
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

old_partitions() ->
  gen_server:call(membership, old_partitions).

stop() ->
  gen_server:call(membership, stop).

fire_gossip(Node) when is_atom(Node) ->
	case net_adm:ping(Node) of
    pang ->
      {error, node_pang};
    pong ->
      gen_server:call(membership, {gossip_with, Node})
  end;

fire_gossip({A1, A2, A3}) ->
  random:seed(A1, A2, A3),
  State = state(),
  % Get all the nodes except for ourself
  case lists:delete(node(), membership:nodes()) of
    [] -> ok; % no other nodes
    Nodes when is_list(Nodes) ->
      fire_gossip(random_node(Nodes))
  end,
  timer:apply_after(random:uniform(5000) + 5000, membership, fire_gossip, [random:seed()]).

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
init(ConfigIn) ->
  process_flag(trap_exit, true),
  Nodes = erlang:nodes(),
  {ok, State} = case load_state(ConfigIn) of
    {ok, Value} -> 
      error_logger:info_msg("loading membership from disk~n", []),
      configuration:set_config(Value#membership.config),
      {ok, Value};
    _ -> if
		  length(Nodes) > 0 -> 
  		  Node = random_node(Nodes),
  		  error_logger:info_msg("joining node ~p~n", [Node]),
  		  join_node(Node, node());
  		true -> {ok, create_initial_state(ConfigIn)}
  	end
  end,
  error_logger:info_msg("Loading storage servers.~n"),
	reload_storage_servers(empty, State),
	error_logger:info_msg("Loading sync servers.~n"),
	reload_sync_servers(empty, State),
	error_logger:info_msg("Starting membership gossip.~n"),
  timer:apply_after(random:uniform(1000) + 1000, membership, fire_gossip, [random:seed()]),
  error_logger:info_msg("Initialized."),
	{ok, State#membership{config=configuration:get_config()}}.

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

handle_call({join_node, Node}, {_, _From}, State = #membership{config=Config}) ->
  error_logger:info_msg("~p is joining the cluster.~n", [node(_From)]),
  NewState = int_join_node(Node, State),
  reload_storage_servers(State, NewState),
  reload_sync_servers(State, NewState),
  save_state(NewState),
	{reply, {ok, NewState}, NewState#membership{config=Config}};

handle_call({gossip_with, Node}, From, State = #membership{nodes = Nodes}) ->
  error_logger:info_msg("firing gossip at ~p~n", [Node]),

  % share our state with target node - expects a response back, but we don't
  % want to wait for it
  Self = self(),
  GosFun =
    fun() ->
        {ok, RemoteState} = gen_server:call({membership, Node}, {share, State}),
        {ok, ModState} = gen_server:call(Self, {merge_state, RemoteState}),
        gen_server:reply(From, {ok, ModState})
    end,
  spawn_link(GosFun),
  {noreply, State};

handle_call({merge_state, RemoteState}, _From, State) ->
  {ok, NewState} = merge_and_save_state(RemoteState, State),
  {reply, {ok, NewState}, NewState};

%%
% Another node is sharing their state with us. Need to merge them in
% and reply with the merged state
%%
handle_call({share, RemoteState}, _From, State = #membership{config=Config}) ->
  {ok, Merged} = merge_and_save_state(RemoteState, State),
  {reply, {ok, Merged}, Merged};
	
handle_call(nodes, _From, State = #membership{nodes=Nodes}) ->
  {reply, Nodes, State};
  
handle_call(state, _From, State) -> {reply, State, State};
	
handle_call(old_partitions, _From, State = #membership{old_partitions=P}) when is_list(P) ->
  {reply, P, State};
  
handle_call(old_partitions, _From, State) -> {reply, [], State};
	
handle_call(partitions, _From, State) -> {reply, State#membership.partitions, State};
	
handle_call({replica_nodes, Node}, _From, State) ->
  {reply, int_replica_nodes(Node, State), State};
	
handle_call({range, Partition}, _From, State) ->
  {reply, int_range(Partition, State#membership.config), State};
	
handle_call({nodes_for_partition, Partition}, _From, State) ->
  {reply, int_nodes_for_partition(Partition, State), State};
	
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
handle_cast(_, State) ->
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

int_range(Partition, #config{q=Q}) ->
  Size = partition_range(Q),
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

load_state(#config{directory=Directory}) ->
  case file:read_file(lists:concat([Directory, "/membership.bin"])) of
    {ok, Binary} -> 
      {ok, binary_to_term(Binary)};
    _ -> not_found
  end. 

save_state(State) ->
  Config = configuration:get_config(),
  Binary = term_to_binary(State),
  {ok, File} = file:open(lists:concat([Config#config.directory, "/membership.bin"]), [write, raw]),
  ok = file:write(File, Binary),
  ok = file:close(File).

%% partitions is a list starting with 1 which defines a partition space.
create_initial_state(Config) ->
  Q = Config#config.q,
  #membership{
    version=vector_clock:create(node()),
	  partitions=create_partitions(Q, node()),
	  config=Config,
	  nodes=[node()]}.
	
create_partitions(Q, Node) ->
  lists:map(fun(Partition) -> {Node, Partition} end, lists:seq(1, ?power_2(32), partition_range(Q))).
	
partition_range(Q) -> ?power_2(32-Q).

merge_states(StateA, StateB) ->
  PartA = StateA#membership.partitions,
  PartB = StateB#membership.partitions,
  Config = StateB#membership.config,
  Nodes = lists:usort(lists:merge(StateA#membership.nodes, StateB#membership.nodes)),
  Partitions = partitions:merge_partitions(PartA, PartB, Config#config.n, Nodes),
  % error_logger:info_msg("Merged nodes ~p and partitions ~p~n", [Nodes, length(Partitions)]),
  #membership{
    version=vector_clock:merge(StateA#membership.version, StateB#membership.version),
    nodes=Nodes,
    partitions=Partitions,
    config=Config
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
        RemoteState;
      greater -> % remote state is strictly older
        State;
      equal -> % same vector clock
        State;
      concurrent -> % must merge
        merge_states(RemoteState, State)
  end,
  NewState = Merged#membership{config = State#membership.config},
  reload_storage_servers(State, NewState),
  reload_sync_servers(State, NewState),
  save_state(NewState),
  {ok, NewState}.


reload_sync_servers(empty, NewState) ->
  Config = configuration:get_config(),
  Partitions = int_partitions_for_node(node(), NewState, master),
  reload_sync_servers([], Partitions, Config);
  
reload_sync_servers(OldState, NewState) ->
  Config = configuration:get_config(),
  PartForNode = int_partitions_for_node(node(), NewState, master),
  OldPartForNode = int_partitions_for_node(node(), OldState, master),
  Old = OldState#membership.partitions,
  NewPartitions = lists:filter(fun(E) ->
      not lists:member(E, OldPartForNode)
    end, PartForNode),
  OldPartitions = lists:filter(fun(E) ->
      not lists:member(E, PartForNode)
    end, OldPartForNode),
  reload_sync_servers(OldPartitions, NewPartitions, Config).
  
reload_sync_servers(_, _, #config{live=Live}) when not Live ->
  ok;
  
reload_sync_servers(OldParts, NewParts, Config) ->
  lists:foreach(fun(E) ->
      Name = list_to_atom(lists:concat([sync_, E])),
      supervisor:terminate_child(sync_server_sup, Name),
      supervisor:delete_child(sync_server_sup, Name)
    end, OldParts),
  lists:foreach(fun(Part) ->
      Name = list_to_atom(lists:concat([sync_, Part])),
      Spec = {Name, {sync_server, start_link, [Name, Part]}, permanent, 1000, worker, [sync_server]},
      case supervisor:start_child(sync_server_sup, Spec) of
        already_present -> supervisor:restart_child(sync_server_sup, Name);
        _ -> ok
      end
    end, NewParts).

reload_storage_servers(empty, NewState) ->
  Config = configuration:get_config(),
  error_logger:info_msg("state: ~p~n", [NewState]),
  Partitions = int_partitions_for_node(node(), NewState, all),
  error_logger:info_msg("Partitions: ~p~n", [Partitions]),
  Old = NewState#membership.old_partitions,
  reload_storage_servers([], Partitions, Old, Config);

reload_storage_servers(OldState, NewState) ->
  Config = configuration:get_config(),
  PartForNode = int_partitions_for_node(node(), NewState, all),
  OldPartForNode = int_partitions_for_node(node(), OldState, all),
  Old = OldState#membership.partitions,
  NewPartitions = lists:filter(fun(E) ->
      not lists:member(E, OldPartForNode)
    end, PartForNode),
  OldPartitions = lists:filter(fun(E) ->
      not lists:member(E, PartForNode)
    end, OldPartForNode),
  reload_storage_servers(OldPartitions, NewPartitions, Old, Config).
  
reload_storage_servers(_, _, _, #config{live=Live}) when not Live ->
  ok;
  
reload_storage_servers(OldParts, NewParts, Old, Config = #config{live=Live}) when Live ->
  lists:foreach(fun(E) ->
      Name = list_to_atom(lists:concat([storage_, E])),
      supervisor:terminate_child(storage_server_sup, Name),
      supervisor:delete_child(storage_server_sup, Name)
    end, OldParts),
  lists:foreach(fun(Part) ->
    Name = list_to_atom(lists:concat([storage_, Part])),
    DbKey = lists:concat([Config#config.directory, "/", Part]),
    BlockSize = Config#config.blocksize,
    {Min,Max} = int_range(Part, Config),
    Spec = case catch lists:keysearch(Part, 2, Old) of
      {value, {OldNode, _}} -> {Name, {storage_server,start_link,[Config#config.storage_mod, DbKey, Name, Min, Max, BlockSize, OldNode]}, permanent, 1000, worker, [storage_server]};
      _ -> {Name, {storage_server,start_link,[Config#config.storage_mod, DbKey, Name, Min, Max, BlockSize]}, permanent, 1000, worker, [storage_server]}
    end,
    case supervisor:start_child(storage_server_sup, Spec) of
      already_present -> supervisor:restart_child(storage_server_sup, Name);
      _ -> ok
    end
  end, NewParts).

int_join_node(NewNode, #membership{config=Config,partitions=Partitions,version=Version,nodes=OldNodes}) ->
  % Make sure the new node isn't already in the partition table
  % since this screws things up royally at the moment.
  false = lists:keysearch(NewNode, 1, Partitions),

  Nodes = lists:sort([NewNode|OldNodes]),
  P = partitions:rebalance_partitions(NewNode, Nodes, Partitions),
  #membership{config=Config,
    partitions=P,
    version = vector_clock:increment(node(), Version),
    nodes=Nodes,
    old_partitions=Partitions}.
  
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
  Config = State#membership.config,
  n_nodes(Node, Config#config.n, lists:reverse(State#membership.nodes)).
  
int_replica_nodes(Node, State) ->
  Config = State#membership.config,
  n_nodes(Node, Config#config.n, State#membership.nodes).
  
int_nodes_for_key(Key, State) ->
  % error_logger:info_msg("inside int_nodes_for_key~n", []),
  KeyHash = lib_misc:hash(Key),
  Config = State#membership.config,
  Q = Config#config.q,
  Partition = find_partition(KeyHash, Q),
  % error_logger:info_msg("found partition ~w for key ~p~n", [Partition, Key]),
  int_nodes_for_partition(Partition, State).
  
int_nodes_for_partition(Partition, State) ->
  Config = State#membership.config,
  Partitions = State#membership.partitions,
  Q = Config#config.q,
  N = Config#config.n,
  {Node,Partition} = lists:nth(index_for_partition(Partition, Q), Partitions),
  % error_logger:info_msg("Node ~w Partition ~w N ~w~n", [Node, Partition, N]),
  int_replica_nodes(Node, State).
  
int_partition_for_key(Key, State) ->
  KeyHash = lib_misc:hash(Key),
  Config = State#membership.config,
  Q = Config#config.q,
  find_partition(KeyHash, Q).
  
find_partition(Hash, Q) ->
  Size = partition_range(Q),
  Factor = (Hash div Size),
  Rem = (Hash rem Size),
  if
    Rem > 0 -> Factor * Size + 1;
    true -> ((Factor-1) * Size) + 1
  end.
  
%1 based index, thx erlang
index_for_partition(Partition, Q) ->
  Size = partition_range(Q),
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
  
