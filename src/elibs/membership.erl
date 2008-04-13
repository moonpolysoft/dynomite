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
-export([start_link/0, join_ring/1, hash_ring/0, server_for_key/1]).

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

join_ring(Node) ->
	gen_server:call(membership, {join_ring, Node}).
	
hash_ring() ->
	gen_server:call(membership, hash_ring).
	
server_for_key(Key) ->
	gen_server:call(membership, {server_for_key, Key}).

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
		{ok, create_membership_state([])}.

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

handle_call({join_ring, Node}, _From, State) ->
	int_join_ring(Node, State);

handle_call(hash_ring, _From, State) ->
	{reply, State#membership.hash_ring, State};
	
handle_call({server_for_key, Key}, _From, State) ->
	KeyHash = erlang:phash2(Key),
	{reply, nearest_server(KeyHash, State), State}.

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

create_membership_state(Nodes) ->
	create_membership_state(Nodes, [], dict:new()).

create_membership_state([], HashRing, Table) ->
	#membership{hash_ring=HashRing,member_table=Table};

create_membership_state([Node|Tail], HashRing, Table) ->
	VirtualNodes = virtual_nodes(Node),
	create_membership_state(Tail,
		add_nodes_to_ring(VirtualNodes, HashRing),
		map_nodes_to_table(VirtualNodes, Node, Table)).

int_join_ring(Node, State) ->
	case dict:is_key(Node, State#membership.member_table) of
		true -> {reply, duplicate, State};
		false -> 
			VirtualNodes = virtual_nodes(Node),
			{reply, added, #membership{
				hash_ring=add_nodes_to_ring(VirtualNodes, State#membership.hash_ring),
				member_table=map_nodes_to_table(VirtualNodes, Node, State#membership.member_table)
			}}
	end.

virtual_nodes(Node) ->
	virtual_nodes(Node, 1).
	
virtual_nodes(Node, Bias) ->
	lists:map(
		fun(I) -> 
			erlang:phash2([I|atom_to_list(Node)])
		end,
		lists:seq(1, ?VIRTUALNODES * Bias)
	).
	
map_nodes_to_table(VirtualNodes, Node, Table) ->
	lists:foldl(
		fun(VirtualNode, AccTable) ->
			dict:store(VirtualNode, Node, AccTable)
		end, Table, VirtualNodes
	).
	
add_nodes_to_ring(VirtualNodes, HashRing) ->
	% HashRing should be pre-sorted
	lists:merge(lists:sort(VirtualNodes), HashRing).
	
nearest_server(Code, #membership{hash_ring=Ring, member_table=Table}) ->
	ServerCode = case nearest_server(Code, Ring) of
		first -> nearest_server(0, Ring);
		FoundCode -> FoundCode
	end,
	dict:fetch(ServerCode, Table);
	
nearest_server(Code, [ServerKey|Tail]) ->
	case Code < ServerKey of
		true -> ServerKey;
		false -> nearest_server(Code, Tail)
	end;

nearest_server(_Code, []) -> first.
