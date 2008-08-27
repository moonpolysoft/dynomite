%%%-------------------------------------------------------------------
%%% File:      untitled.erl
%%% @author    Cliff Moon <cliff@powerset.com> []
%%% @copyright 2008 Cliff Moon
%%% @doc  
%%%
%%% @end  
%%%
%%% @since 2008-08-25 by Cliff Moon
%%%-------------------------------------------------------------------
-module(socket_server).
-author('cliff@powerset.com').

-behaviour(gen_server).

%% API
-export([start_link/1, accept_loop/3, connections/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("config.hrl").

-record(socket_server, {max=100,connections=0,acceptor,listen}).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% @spec start_link() -> {ok,Pid} | ignore | {error,Error}
%% @doc Starts the server
%% @end 
%%--------------------------------------------------------------------
start_link(Config) ->
  gen_server:start_link({local, socket_server}, ?MODULE, Config, []).

connections() ->
  gen_server:call(socket_server, connections).

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
  process_flag(trap_exit, true),
  listen(Config, #socket_server{}).

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
handle_call(connections, _From, State = #socket_server{connections=Conn}) ->
  {reply, Conn, State};

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
handle_cast({accepted, Pid}, State = #socket_server{acceptor=Pid,connections=Conn}) ->
  {noreply, spawn_acceptor(State#socket_server{connections=Conn+1})};

handle_cast(stop, State) ->
  {stop, normal, State}.

%%--------------------------------------------------------------------
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% @doc Handling all non call/cast messages
%% @end 
%%--------------------------------------------------------------------
handle_info({'EXIT', Pid, normal}, State=#socket_server{acceptor=Pid}) ->
  {noreply, spawn_acceptor(State)};
    
handle_info({'EXIT', Pid, Reason}, State=#socket_server{acceptor=Pid}) ->
  error_logger:error_report([{?MODULE, ?LINE}, {acceptor_error, Reason}]),
  timer:sleep(100),
  {noreply, spawn_acceptor(State)};
  
handle_info({'EXIT', _LoopPid, Reason}, State=#socket_server{acceptor=Pid, connections=Conn}) ->
  case Reason of
    normal -> ok;
    _ -> error_logger:error_report([{?MODULE, ?LINE}, {child_error, Reason}])
  end,
  State1 = State#socket_server{connections=Conn-1},
  {noreply, case Pid of
      undefined -> spawn_acceptor(State1);
      _ -> State1
    end};
    
handle_info(Info, State) ->
  error_logger:info_report([{'INFO', Info}, {'State', State}]),
  {noreply, State}.

%%--------------------------------------------------------------------
%% @spec terminate(Reason, State) -> void()
%% @doc This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%% @end 
%%--------------------------------------------------------------------
terminate(_Reason, #socket_server{listen=Listen}) ->
    gen_tcp:close(Listen).

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

listen(Config = #config{port=Port}, State) ->
  case gen_tcp:listen(Port, [binary, inet6, {active,false}, {packet, 0}, {reuseaddr, true}]) of
    {ok, Listen} ->
      {ok, spawn_acceptor(State#socket_server{listen=Listen})};
    {error, Reason} ->
      {stop, Reason}
  end.
  
spawn_acceptor(State = #socket_server{max=Max,connections=ConnSize}) when Max == ConnSize ->
  error_logger:info_msg("conn limit of ~p has been reached.", [Max]),
  State#socket_server{acceptor=undefined};

spawn_acceptor(State = #socket_server{listen=Listen}) ->
  Pid = proc_lib:spawn_link(?MODULE, accept_loop, [self(), Listen, State]),
  State#socket_server{acceptor=Pid}.
  
accept_loop(Server, Listen, _State) ->
  case catch gen_tcp:accept(Listen) of
    {ok, Socket} ->
      gen_server:cast(Server, {accepted, self()}),
      connection_loop(Socket);
    {error, closed} ->
      exit({error, closed});
    Other ->
      error_logger:error_report([{application,dynomite},"Accept failed", lists:flatten(io_lib:format("~p", [Other]))]),
      exit({error, accept_failed})
  end.
  
connection_loop(Socket) ->
  case read_section(Socket) of
    {ok, Cmd} ->
      catch execute_command(Cmd, Socket),
      connection_loop(Socket);
    {error, Reason} ->
      gen_tcp:close(Socket),
      exit(shutdown)
  end. 
  
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% command implementation
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
execute_command("get", Socket) ->
  Length = read_length(Socket),
  Key = binary_to_list(read_data(Socket, Length)),
  case mediator:get(Key) of
    {ok, not_found} -> send_not_found(Socket);
    {ok, {Context, Values}} -> send_get(Socket, Context, Values);
    {failure, Reason} -> send_failure(Socket, Reason)
  end;

execute_command("put", Socket) ->
  Key = binary_to_list(read_length_data(Socket)),
  ContextData = read_length_data(Socket),
  Context = if
    erlang:byte_size(ContextData) > 0 -> binary_to_term(ContextData);
    true -> []
  end,
  Data = read_length_data(Socket),
  case mediator:put(Key, Context, Data) of
    {failure, Reason} -> send_failure(Socket, Reason);
    {ok, N} -> send_msg(Socket, "succ", N)
  end;

execute_command("has", Socket) ->
  Key = binary_to_list(read_length_data(Socket)),
  case mediator:has_key(Key) of
    {ok, {true, N}} -> send_msg(Socket, "yes", N);
    {ok, {false, N}} -> send_msg(Socket, "no", N);
    {failure, Reason} -> send_failure(Socket, Reason)
  end;

execute_command("del", Socket) ->
  Key = binary_to_list(read_length_data(Socket)),
  case mediator:delete(Key) of
    {ok, N} -> send_msg(Socket, "succ", N);
    {failure, Reason} -> send_failure(Socket, Reason)
  end;

execute_command("close", Socket) ->
  gen_tcp:send(Socket, "close\n"),
  gen_tcp:close(Socket),
  exit(closed).

send_not_found(Socket) -> gen_tcp:send(Socket, "not_found\n").

send_failure(Socket, Reason) ->
  gen_tcp:send(Socket, "fail "),
  gen_tcp:send(Socket, Reason),
  gen_tcp:send(Socket, "\n").

send_msg(Socket, Msg, N) ->
  gen_tcp:send(Socket, Msg),
  gen_tcp:send(Socket, " "),
  gen_tcp:send(Socket, integer_to_list(N)),
  gen_tcp:send(Socket, "\n").

send_get(Socket, Context, Values) ->
  gen_tcp:send(Socket, "succ "),
  ItemLength = length(Values),
  write_length(Socket, ItemLength),
  ContextBin = term_to_binary(Context),
  write_binary(Socket, ContextBin, " "),
  lists:foldl(fun(Value, Acc) ->
    if
      Acc == ItemLength-1 -> write_binary(Socket, Value, "\n");
      true -> write_binary(Socket, Value, " ")
    end,
    Acc+1
  end, 0, Values).

read_length_data(Socket) ->
  Length = read_length(Socket),
  read_data(Socket, Length).

read_data(Socket, Length) ->
  {ok, Data} = if
    Length == 0 -> {ok, <<"">>};
    true -> gen_tcp:recv(Socket, Length)
  end,
  {ok, _Term} = gen_tcp:recv(Socket, 1),
  Data.

read_length(Socket) ->
  case read_section(Socket) of
    {ok, Blah} -> list_to_integer(Blah);
    {error, Reason} ->
      gen_tcp:close(Socket),
      exit({error, Reason})
  end.

read_section(Socket) ->
  read_section([], Socket).

read_section(Cmd, Socket) ->
  case gen_tcp:recv(Socket, 1) of
    {ok, <<" ">>} -> {ok, lists:reverse(Cmd)};
    {ok, <<"\n">>} -> {ok, lists:reverse(Cmd)};
    {ok, <<Char>>} -> read_section([Char|Cmd], Socket);
    Other -> Other
  end.

write_binary(Socket, Bin, Term) ->
  Length = erlang:byte_size(Bin),
  write_length(Socket, Length),
  gen_tcp:send(Socket, Bin),
  gen_tcp:send(Socket, Term).

write_length(Socket, Length) ->
  gen_tcp:send(Socket, integer_to_list(Length)),
  gen_tcp:send(Socket, " ").  