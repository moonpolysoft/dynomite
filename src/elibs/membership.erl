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
-author('').

-behaviour(gen_server).

-define(SERVER, membership).
-define(VIRTUALNODES, 100).

%% API
-export([start_link/0, join_ring/1, hash_ring/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(membership, {hash_ring, member_table}).


%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% @spec start_link() -> {ok,Pid} | ignore | {error,Error}
%% @doc Starts the server
%% @end 
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({global, ?SERVER}, ?MODULE, [], []).

join_ring(Node) ->
	gen_server:call({global, ?SERVER}, {join_ring, Node}).
	
hash_ring() ->
	gen_server:call({global, ?SERVER}, hash_ring).

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
		VirtualNodes = virtual_nodes(node()),
		Table = map_nodes_to_table(VirtualNodes, node(), dict:new()),
		HashRing = add_nodes_to_ring(VirtualNodes, []),
    {ok, #membership{hash_ring = HashRing, member_table = Table}}.

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
	case dict:is_key(Node, State#membership.member_table) of
		true -> {reply, duplicate, State};
		false -> 
			VirtualNodes = virtual_nodes(Node),
			{reply, added, #membership{
				hash_ring=add_nodes_to_ring(VirtualNodes, State#membership.hash_ring),
				member_table=map_nodes_to_table(VirtualNodes, Node, State#membership.member_table)
			}}
	end;

handle_call(hash_ring, _From, State) ->
	{reply, State#membership.hash_ring, State}.

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