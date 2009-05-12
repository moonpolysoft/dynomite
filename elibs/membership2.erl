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

-include("../include/config.hrl").
-include("../include/common.hrl").

-record(state, {header=?VERSION, node, nodes, partitions, version, servers}).

-ifdef(TEST).
-include("etest/membership2_test.erl").
-endif.

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
  
register(Partition, Pid) ->
  gen_server:cast(membership, {register, Partition, Pid}).
  
servers_for_key(Key) ->
  gen_server:call(membership, {servers_for_key, Key}).
  
stop(Server) ->
  gen_server:cast(Server, stop).

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
  Servers = ets:new(member_servers, [public, bag]),
  ?debugFmt("so far ~p", [{PartialNodes, Partners, Servers}]),
  {Version, RemoteNodes} = join_to(Node, Servers, Partners),
  WorldNodes = lists:usort(PartialNodes ++ RemoteNodes),
  ?debugFmt("~p", [{Version, WorldNodes}]),
  PMap = partitions:create_partitions(Config#config.q, Node, WorldNodes),
  State = #state{
    node=Node,
    nodes=WorldNodes,
    partitions=PMap,
    version=vector_clock:increment(pid_to_list(self()), Version),
    servers=Servers},
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
handle_call({join, OtherNode}, _From, State = #state{version=Version, node=Node, nodes=Nodes, servers=Servers}) ->
  Config = configuration:get_config(),
  WorldNodes = lists:usort(Nodes ++ [OtherNode]),
  PMap = partition:create_partitions(Config#config.q, Node, WorldNodes),
  ServerList = servers_to_list(Servers),
  NewState = State#state{nodes=WorldNodes, partitions=PMap},
  fire_gossip(Node, NewState, Config),
  {reply, {Version, WorldNodes, ServerList}, NewState};

handle_call({servers_for_key, Key}, _From, State = #state{servers=Servers}) ->
  Config = configuration:get_config(),
  Hash = lib_misc:hash(Key),
  Partition = hash_to_partition(Hash, Config#config.q),
  ?debugFmt("getting for partition ~p", [Partition]),
  {_, Pids} = lists:unzip(ets:lookup(Servers, Partition)),
  {reply, Pids, State};

handle_call(state, _From, State) ->
  {reply, State, State}.

%%--------------------------------------------------------------------
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% @doc Handling cast messages
%% @end 
%%--------------------------------------------------------------------
handle_cast({gossip, Version, Nodes, ServerList}, State = #state{node=Me}) ->
  {MergeType, Merged} = merge_state(Version, Nodes, ServerList, State),
  case MergeType of
    equal -> {noreply, Merged};
    merged ->
      fire_gossip(Me, Merged, configuration:get_config()),
      {noreply, Merged}
  end;

handle_cast({register, Partition, Pid}, State = #state{servers=Servers,node=Me}) ->
  ?debugFmt("registering ~p", [{Partition, Pid}]),
  Ref = erlang:monitor(process, Pid),
  ets:insert(Servers, {Partition, Pid}),
  ets:insert(Servers, {Ref, Partition, Pid}),
  fire_gossip(Me, State, configuration:get_config()),
  {noreply, State};
  
handle_cast(stop, State) ->
  {stop, shutdown, State}.

%%--------------------------------------------------------------------
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% @doc Handling all non call/cast messages
%% @end 
%%--------------------------------------------------------------------
handle_info({'DOWN', Ref, _, Pid, _}, State = #state{node=Me,servers=Servers}) ->
  erlang:demonitor(Ref),
  [{Ref, Partition, Pid}] = ets:lookup(Servers, Ref),
  ets:delete(Servers, Ref),
  ets:delete_object(Servers, {Partition, Pid}),
  ?debugFmt("Pid is down ~p", [Pid]),
  fire_gossip(Me, State, configuration:get_config()),
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
  Config = configuration:get_config(),
  case file:consult(filename:join([Config#config.directory, lists:concat([node:name(Node), ".world"])])) of
    {error, Reason} -> 
      ?infoFmt("Could not load state: ~p~n", [Reason]),
      [];
    {ok, [Terms]} ->
      Terms
  end.

%% save the list of known nodes to a file
save(State) ->
  Config = configuration:get_config(),
  Filename = filename:join([Config#config.directory, lists:concat([node:name(State#state.node), ".world"])]),
  ?debugFmt("opening file ~p", [Filename]),
  {ok, File} = file:open(Filename, [binary, write]),
  io:format(File, "~w.~n", [State#state.nodes]),
  file:close(File).

%% joining is bi-directional, as opposed to gossip which is unidirectional
%% we want to collect the list of known nodes to compute the partition map
%% which isn't necessarily the same as the list of running nodes
join_to(Node, Servers, Partners) ->
  join_to(Node, Servers, Partners, {vector_clock:create(pid_to_list(self())), []}).
  
join_to(_, _, [], {Version, World}) ->
  {Version, World};
join_to(Node, Servers, [Remote|Partners], {Version, World}) ->
  case call_join(Remote, Node) of
    {'EXIT', _} -> World;
    {RemoteVersion, NewNodes, ServerList} ->
      server_list_into_table(ServerList, Servers),
      join_to(Node, Servers, Partners, {
        vector_clock:merge(Version, RemoteVersion), 
        lists:usort(World ++ NewNodes)});
    _Val ->
      ?infoFmt("got ~p back~n", [_Val])
  end.

call_join(Remote, Node) ->
  catch gen_server:call({membership, node:name(Remote)}, {join, Node}).

merge_state(RemoteVersion, RemoteNodes, RemoteServerList, State = #state{nodes = Nodes, version = LocalVersion, servers = Servers}) ->
  case vector_clock:compare(RemoteVersion, LocalVersion) of
    equal -> {equal, State};
    _ ->
      MergedNodes = lists:usort(RemoteNodes ++ Nodes),
      server_list_into_table(RemoteServerList, Servers),
      MergedClock = vector_clock:merge(RemoteVersion, LocalVersion),
      {merged, State#state{nodes=MergedNodes,version=MergedClock}}
  end.

fire_gossip(Me, State = #state{nodes = Nodes}, Config) ->
  Partners = replication:partners(Me, Nodes, Config),
  lists:foreach(fun(Node) -> gossip_with(Me, Node, State) end, Partners).

gossip_with(Me, OtherNode, State = #state{version = Version, nodes = Nodes, servers = Servers}) ->
  ServerPacket = servers_to_list(Servers),
  gen_server:call({membership, OtherNode}, {gossip, Version, Nodes, ServerPacket}).
  
%% this gets everything we know of, not just locals
servers_to_list(Servers) ->
  L = ets:foldl(fun
    ({Partition, Pid}, List) ->
      [{Partition, Pid}|List];
    ({Ref, Partition, Pid}, List) ->
      List
    end, [], Servers),
  lists:keysort(1, L).

server_list_into_table(ServerList, Servers) ->
  lists:foreach(fun({Partition, Pid}) ->
      ets:insert(Servers, {Partition, Pid})
    end, ServerList).
  
hash_to_partition(0, _) ->
  1;
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