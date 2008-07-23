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
-export([start_link/1, join_ring/1, server_for_key/1, server_for_key/2, stop/0, mark_as_bad/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(membership, {config, partitions, version}).

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
  gen_server:start({local, membership}, ?MODULE, Config, []).

join_ring(Node) ->
	gen_server:multi_call({membership, Node}, {join_ring, node()}).
	
server_for_key(Key) ->
  gen_server:call(membership, {server_for_key, Key, 1}).
	
server_for_key(Key, N) ->
	gen_server:call(membership, {server_for_key, Key, N}).

stop() ->
  gen_server:call(membership, stop).

mark_as_bad(BadServers) ->
	gen_server:multi_call(membership, {mark_as_bad, BadServers}).
	
fire_gossip() ->
  gen_server:call(membership, fire_gossip).

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
init(Config) ->
  Nodes = nodes(),
	{ok, State} = if
		length(Nodes) > 0 -> join_ring(random_node());
		true -> {ok, create_initial_state(Config)}
	end,
	timer:apply_interval(500, membership, fire_gossip, []),
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

handle_call({join, Node}, _From, State) ->
  NewState = join_node(Node, State, ordered_nodes()),
	{reply, NewState, NewState};
	
handle_call({share, NewState}, _From, State) ->
  case vector_clock:compare(State#membership.version, NewState#membership.version) of
    less -> {reply, replaced, NewState};
    greater -> {reply, State, State};
    equal -> {reply, replaced, State};
    concurrent -> Merged = merge_states(State, NewState),
      {reply, Merged, Merged}
  end;
	
handle_call(fire_gossip, _From, State) ->
  {Replies,BadNodes} = gen_server:multi_call(random_nodes(2), membership, {share, State}),
  Merged = lists:foldl(fun({_,S}, M) -> merge_states(M, S) end, Replies, State),
  {reply, ok, Merged};
	
handle_call({server_for_key, Key, N}, _From, State) ->
	{reply, noop, State};
	
handle_call({mark_as_bad, BadServers}, _From, State) ->
	{reply, blah, State};
	
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

random_node() -> 
  [Node] = random_nodes(1),
  Node.
  
random_nodes(N) -> random_nodes(N, nodes(), []).
  
random_nodes(_, [], Taken) -> Taken;
  
random_nodes(0, _, Taken) -> Taken;
  
random_nodes(N, Nodes, Taken) ->
  {One, Two} = lists:split(random:uniform(length(Nodes)), Nodes),
  if
    length(Two) > 0 -> 
      [Head|Split] = Two,
      random_nodes(N-1, One ++ Split, [Head|Taken]);
    true ->
      [Head|Split] = One,
      random_nodes(N-1, Split, [Head|Taken])
  end.

ordered_nodes() -> lists:sort(nodes([this, visible])).

%% partitions is a list starting with 0 which defines a partition space.
create_initial_state(Config) ->
  Q = Config#config.q,
  #membership{
    version=vector_clock:create(node()),
	  partitions=create_partitions(Q, node()),
	  config=Config}.
	
create_partitions(Q, Node) ->
  lists:map(fun(Partition) -> {Node, Partition} end, lists:seq(0, ?power_2(32)-1, partition_range(Q))).
	
partition_range(Q) -> ?power_2(32-Q).

merge_states(StateA, StateB) ->
  PartA = StateA#membership.partitions,
  PartB = StateB#membership.partitions,
  Config = StateA#membership.config,
  merge_partitions(PartA, PartB, [], Config#config.n).
  
merge_partitions([], [], Result, _) -> lists:reverse(Result);

merge_partitions([{NodeA,Number}|PartA], [{NodeB,Number}|PartB], Result, N) ->
  if
    NodeA == NodeB -> 
      merge_partitions(PartA, PartB, [{NodeA,Number}|Result], N);
    true ->
      case within(N, NodeA, NodeB, ordered_nodes()) of
        {true, First} -> merge_partitions(PartA, PartB, [{First,Number}|Result], N);
        % bah, maybe we should just fucking pick one
        _ -> merge_partitions(PartA, PartB, [{NodeA,Number}|Result], N)
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

join_node(NewNode, #membership{config=Config,partitions=Partitions,version=Version}, Nodes) ->
  Tokens = ?power_2(Config#config.q) div length(Nodes),
  FromEachNode = Tokens div (length(Nodes)-1),
  {CleanNodes,_} = lists:partition(fun(E) -> E =/= NewNode end, Nodes),
  #membership{config=Config,
    partitions = steal_partitions(NewNode, Tokens, FromEachNode, FromEachNode, CleanNodes, Partitions, []),
    version = vector_clock:increment(node(), Version)}.
  
steal_partitions(_, 0, _, _, _, Partitions, Stolen) ->
  lists:keysort(2, Partitions ++ Stolen);
  
steal_partitions(Node, Tokens, 0, FromEach, [Head|Nodes], Partitions, Stolen) ->
  if
    length(Nodes) > 0 -> steal_partitions(Node, Tokens, FromEach, FromEach, Nodes, Partitions, Stolen);
    true -> steal_partitions(Node, Tokens, FromEach, FromEach, [Head|Nodes], Partitions, Stolen)
  end;
  
steal_partitions(Node, Tokens, FromThis, FromEach, [Head|Nodes], Partitions, Stolen) ->
  case lists:keytake(Head, 1, Partitions) of
    {value, {Head, Partition}, NewPartitions} ->
      steal_partitions(Node, Tokens-1, FromThis-1, FromEach, [Head|Nodes], NewPartitions, [{Node,Partition}|Stolen]);
    % there may be another node joining the system in this case.  we should handle gracefully
    false -> steal_partitions(Node, Tokens, FromThis, FromEach, [Head|Nodes], Partitions, Stolen)
  end.
  
  
  
  
  
  