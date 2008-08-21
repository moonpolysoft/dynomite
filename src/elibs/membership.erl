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
-export([start_link/1, join_node/2, nodes_for_key/1, partitions/0, nodes/0, state/0, state/1, old_partitions/0, partitions_for_node/2, fire_gossip/1, partition_for_key/1, stop/0, range/1]).

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
	
nodes_for_key(Key) ->
  gen_server:call(membership, {nodes_for_key, Key}).
  
nodes() ->
  gen_server:call(membership, nodes).
  
state() ->
  gen_server:call(membership, state).
  
state(State) ->
  gen_server:call(membership, {state, State}).
  
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
	
fire_gossip({A1, A2, A3}) ->
  random:seed(A1, A2, A3),
  State = state(),
  Config = State#membership.config,
  Nodes = lists:filter(fun(E) -> E /= node() end, membership:nodes()),
  ModState = if
    length(Nodes) > 0 ->
      Node = random_node(Nodes),
        % error_logger:info_msg("firing gossip at ~p~n", [Node]),
      RemoteState = gen_server:call({membership, Node}, {share, State}),
      merge_states(RemoteState, State);
    true -> State
  end,
  membership:state(ModState#membership{config=Config}),
  reload_storage_servers(State, ModState),
  save_state(ModState),
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
	reload_storage_servers(empty, State),
  timer:apply_after(random:uniform(1000) + 1000, membership, fire_gossip, [random:seed()]),
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
  save_state(NewState),
	{reply, {ok, NewState}, NewState#membership{config=Config}};
	
handle_call({share, NewState}, _From, State = #membership{config=Config}) ->
  % error_logger:info_msg("sharing state ~p ~p~n", [State#membership.config, NewState#membership.config]),
  case vector_clock:compare(State#membership.version, NewState#membership.version) of
    less -> 
      reload_storage_servers(State, NewState),
      {reply, NewState, NewState};
    greater -> {reply, State, State};
    equal -> {reply, State, State};
    concurrent -> 
      Merged = merge_states(NewState, State),
      reload_storage_servers(State, Merged),
      save_state(Merged),
      {reply, Merged, Merged#membership{config=State#membership.config}}
  end;
	
handle_call(nodes, _From, State = #membership{nodes=Nodes}) ->
  {reply, Nodes, State};
  
handle_call(state, _From, State) -> {reply, State, State};

handle_call({state, NewState}, _From, State = #membership{config=Config}) -> {reply, ok, NewState#membership{config=Config}};
	
handle_call(old_partitions, _From, State = #membership{old_partitions=P}) when is_list(P) ->
  {reply, P, State};
  
handle_call(old_partitions, _From, State) -> {reply, [], State};
	
handle_call(partitions, _From, State) -> {reply, State#membership.partitions, State};
	
handle_call({range, Partition}, _From, State) ->
  {reply, int_range(Partition, State#membership.config), State};
	
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
  Partitions = merge_partitions(PartA, PartB, [], Config#config.n, Nodes),
  % error_logger:info_msg("Merged nodes ~p and partitions ~p~n", [Nodes, length(Partitions)]),
  #membership{
    version=vector_clock:merge(StateA#membership.version, StateB#membership.version),
    nodes=Nodes,
    partitions=Partitions,
    config=Config
  }.
  
merge_partitions([], [], Result, _, _) -> lists:keysort(2, Result);
merge_partitions(A, [], Result, _, _) -> lists:keysort(2, A ++ Result);
merge_partitions([], B, Result, _, _) -> lists:keysort(2, B ++ Result);

merge_partitions([{NodeA,Number}|PartA], [{NodeB,Number}|PartB], Result, N, Nodes) ->
  if
    NodeA == NodeB -> 
      merge_partitions(PartA, PartB, [{NodeA,Number}|Result], N, Nodes);
    true ->
      case within(N, NodeA, NodeB, Nodes) of
        {true, First} -> merge_partitions(PartA, PartB, [{First,Number}|Result], N, Nodes);
        % bah, maybe we should just fucking pick one
        _ -> merge_partitions(PartA, PartB, [{NodeA,Number}|Result], N, Nodes)
      end
  end.

within(N, NodeA, NodeB, Nodes) ->
  within(N, NodeA, NodeB, Nodes, nil).

within(_, _, _, [], _) -> false;
  
within(N, NodeA, NodeB, [Head|Nodes], nil) ->
  case Head of
    NodeA -> within(N-1, NodeB, nil, Nodes, NodeA);
    NodeB -> within(N-1, NodeA, nil, Nodes, NodeB);
    _ -> within(N-1, NodeA, NodeB, Nodes, nil)
  end;
  
within(0, _, _, _, _) -> false;
  
within(N, Last, nil, [Head|Nodes], First) ->
  case Head of
    Last -> {true, First};
    _ -> within(N-1, Last, nil, Nodes, First)
  end.

reload_storage_servers(empty, NewState) ->
  Config = configuration:get_config(),
  Partitions = int_partitions_for_node(node(), NewState, all),
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
  Nodes = lists:sort([NewNode|OldNodes]),
  P = steal_partitions(NewNode, Partitions, Nodes, Config),
  #membership{config=Config,
    partitions=P,
    version = vector_clock:increment(node(), Version),
    nodes=Nodes,
    old_partitions=Partitions}.
  
steal_partitions(ForNode, Partitions, Nodes, #config{q=Q}) ->
  Tokens = ?power_2(Q) div length(Nodes),
  FromEach = Tokens div (length(Nodes)-1),
  case lists:keysearch(ForNode, 1, Partitions) of
    {value, _} -> Partitions;
    false -> if
      FromEach == 0 -> steal_partitions(ForNode, Tokens, 1, Partitions, Nodes, []);
      true -> steal_partitions(ForNode, Tokens, FromEach, Partitions, Nodes, [])
    end
  end.
  
steal_partitions(_, _, _, Partitions, [], Stolen) ->
  % error_logger:info_msg("ran out of nodes ~n", []),
  lists:keysort(2, Partitions ++ Stolen);  

steal_partitions(ForNode, Tokens, FromEach, Partitions, Nodes, Stolen) when length(Stolen) == Tokens ->
  % error_logger:info_msg("got enough stolen ~p~n", [Stolen]),
  timer:sleep(100),
  lists:keysort(2, Partitions ++ Stolen);

% skip fornode
steal_partitions(ForNode, Tokens, FromEach, Partitions, [ForNode|Nodes], Stolen) ->
  % error_logger:info_msg("fornode ~p tokens ~p fromeach ~p partitions ~p nodes ~p stolen ~p~n", [ForNode, Tokens, FromEach, length(Partitions), Nodes, length(Stolen)]),
  steal_partitions(ForNode, Tokens, FromEach, Partitions, Nodes, Stolen);

steal_partitions(ForNode, Tokens, FromEach, Partitions, [FromNode|Nodes], Stolen) ->
  % error_logger:info_msg("fornode ~p tokens ~p fromeach ~p partitions ~p nodes ~p stolen ~p~n", [ForNode, Tokens, FromEach, length(Partitions), Nodes, length(Stolen)]),
  {NewPartitions,NewStolen} = steal_n(FromEach, FromNode, ForNode, Partitions, Stolen),
  steal_partitions(ForNode, Tokens, FromEach, NewPartitions, Nodes, NewStolen).
  
steal_n(0, Node, ForNode, Partitions, Stolen) -> {Partitions,Stolen};

% we want to make sure to always leave at least on partition for an existing node
steal_n(N, Node, ForNode, Partitions, Stolen) ->
  case lists:keytake(Node, 1, Partitions) of
    {value, {_,Part}, NewPartitions} -> 
      case lists:keytake(Node, 1, NewPartitions) of
        {value, _, _} -> steal_n(N-1, Node, ForNode, NewPartitions, [{ForNode,Part}|Stolen]);
        false -> {Partitions,Stolen}
      end;
    false -> {Partitions,Stolen}
  end.
  
int_partitions_for_node(Node, State, master) ->
  Partitions = State#membership.partitions,
  {Matching,_} = lists:partition(fun({N,_}) -> N == Node end, Partitions),
  lists:map(fun({_,P}) -> P end, Matching);
  
int_partitions_for_node(Node, State, all) ->
  Config = State#membership.config,
  Partitions = State#membership.partitions,
  Nodes = n_nodes(Node, Config#config.n, lists:reverse(State#membership.nodes)),
  lists:foldl(fun(E, Acc) -> 
      lists:merge(Acc, int_partitions_for_node(E, State, master)) 
    end, [], Nodes).
  
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
  n_nodes(Node, N, State#membership.nodes).
  
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
  
