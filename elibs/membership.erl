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
-export([start_link/0, join_ring/1, hash_ring/0, server_for_key/1, server_for_key/2, stop/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(membership, {hash_ring, member_table}).

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
start_link() ->
  gen_server:start({local, membership}, ?MODULE, [], []).

join_ring(Server) ->
	gen_server:multi_call(membership, {join_ring, Server}).
	
hash_ring() ->
	gen_server:call(membership, hash_ring).
	
server_for_key(Key) ->
  gen_server:call(membership, {server_for_key, Key, 1}).
	
server_for_key(Key, N) ->
	gen_server:call(membership, {server_for_key, Key, N}).

stop() ->
  gen_server:call(membership, stop).

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
init(_Args) ->
    State = create_membership_state([]),
    case length(nodes()) > 0 of
      true -> {Replies,_BadNodes} = gen_server:multi_call(nodes(), membership, {merge_rings, State}),
        [{_Node, MergedState}|_] = Replies,
        {ok, MergedState};
      false -> {ok, State}
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

handle_call({join_ring, {Name, Node}}, _From, State) ->
	{Added, NewState} = int_join_ring({Name, Node}, State),
	{reply, Added, NewState};

handle_call(hash_ring, _From, State) ->
	{reply, State#membership.hash_ring, State};
	
handle_call({merge_rings, OutsideState}, _From, State) ->
  {reply, State, int_merge_states(OutsideState, State)};
	
handle_call({server_for_key, Key, N}, _From, State) ->
	KeyHash = erlang:phash2(Key),
	{reply, nearest_server(KeyHash, N, State), State};
	
handle_call(stop, _From, State) ->
  {stop, shutdown, ok, State}.

%%--------------------------------------------------------------------
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% @doc Handling cast messages
%% @end 
%%--------------------------------------------------------------------
handle_cast({join_ring, Node}, State) ->
		{_Added, NewState} = int_join_ring(Node, State),
    {noreply, NewState}.

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
create_membership_state(Nodes) ->
	create_membership_state(Nodes, [], dict:new()).

create_membership_state([], HashRing, Table) ->
	#membership{hash_ring=HashRing,member_table=Table};

create_membership_state([Node|Tail], HashRing, Table) ->
	VirtualNodes = virtual_nodes(Node),
	create_membership_state(Tail,
		add_nodes_to_ring(VirtualNodes, HashRing),
		map_nodes_to_table(VirtualNodes, Node, Table)).

int_join_ring(ServerName, State) ->
  case server_in_ring(ServerName, State) of
		true -> {duplicate, State};
		false -> 
			VirtualNodes = virtual_nodes(ServerName),
			{added, #membership{
				hash_ring=add_nodes_to_ring(VirtualNodes, State#membership.hash_ring),
				member_table=map_nodes_to_table(VirtualNodes, ServerName, State#membership.member_table)
			}}
	end.
	
int_remove_node(ServerName, State) ->
  case server_in_ring(ServerName, State) of
    false -> {not_found, State};
    true ->
      VirtualNodes = virtual_nodes(ServerName),
      {removed, #membership{
        hash_ring=remove_nodes_from_ring(VirtualNodes, State#membership.hash_ring),
        member_table=remove_nodes_from_table(VirtualNodes, State#membership.member_table)
      }}
  end.
	
int_merge_states(#membership{member_table=TableA,hash_ring=RingA}, #membership{member_table=TableB,hash_ring=RingB}) ->
  MergedDict = dict:merge(fun(_Key,Value1,_Value2) -> 
      Value1 
    end, TableA, TableB),
  MergedRing = lists:umerge(RingA,RingB),
  #membership{member_table=MergedDict,hash_ring=MergedRing}.
	
server_in_ring(ServerName, #membership{member_table=Table}) ->
  [ServerKey|_] = virtual_nodes(ServerName),
  dict:is_key(ServerKey, Table).
  
virtual_nodes(NameNode) ->
	virtual_nodes(NameNode, 1).
	
virtual_nodes({Name, Node}, Bias) ->
  AbsName = lists:concat([Name, "/", Node]),
	virtual_nodes(AbsName, Bias);
	
virtual_nodes(ServerName, Bias) ->
  AbsName = lists:concat([ServerName]),
  lists:map(
    fun(I) ->
      erlang:phash2([I|AbsName])
    end,
    lists:seq(1, ?VIRTUALNODES * Bias)
  ).
	
map_nodes_to_table(VirtualNodes, Node, Table) ->
	lists:foldl(
		fun(VirtualNode, AccTable) ->
			dict:store(VirtualNode, Node, AccTable)
		end, Table, VirtualNodes
	).
	
remove_nodes_from_ring(VirtualNodes, Ring) ->
  lists:subtract(Ring, VirtualNodes).
  
remove_nodes_from_table(VirtualNodes, Table) ->
  lists:foldl(
		fun(VirtualNode, AccTable) ->
			dict:erase(VirtualNode, AccTable)
		end, Table, VirtualNodes
	).
	
add_nodes_to_ring(VirtualNodes, HashRing) ->
	% HashRing should be pre-sorted
	lists:merge(lists:sort(VirtualNodes), HashRing).

nearest_server(Code, N, State) ->
  nearest_server(Code, N, State, []).
	
nearest_server(_Code, 0, _State, _AlreadyFound) -> [];
	%wrong, this needs to return 3 distinct servers
nearest_server(Code, N, State, AlreadyFound) ->
  #membership{hash_ring=Ring, member_table=Table} = State,
	ServerCode = case nearest_server(Code, Ring) of
		first -> nearest_server(0, Ring);
		FoundCode -> FoundCode
	end,
	ServerName = dict:fetch(ServerCode, Table),
	{removed, ModState} = int_remove_node(ServerName, State),
	[dict:fetch(ServerCode, Table) | nearest_server(ServerCode, N-1, ModState)].
	
nearest_server(Code, [ServerKey|Tail]) ->
	case Code < ServerKey of
		true -> ServerKey;
		false -> nearest_server(Code, Tail)
	end;

nearest_server(_Code, []) -> first.
