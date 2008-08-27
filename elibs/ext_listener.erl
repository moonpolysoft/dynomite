-module (ext_listener).
-export ([start_link/1, loop/1]).

-ifdef(TEST).
-include("etest/ext_listener_test.erl").
-endif.

start_link(Config) ->
  Pid = spawn(fun() -> init(Config) end),
  link(Pid),
  {ok, Pid}.
  
%%---------------------------------
% internal functions

init(Config) ->
  Port = case application:get_env(port) of
    {ok, Val} -> Val;
    undefined -> 11222
  end,
  {ok, Listen} = gen_tcp:listen(Port, [binary, inet6, {active, false}, {packet, 0}, {reuseaddr, true}]),
  error_logger:info_msg("listening on ~p~n", [Port]),
  par_connect(Listen).
  
par_connect(Listen) ->
  {ok, Socket} = gen_tcp:accept(Listen),
  error_logger:info_msg("got connection~n"),
  spawn_opt(ext_listener, loop, [Socket], [{fullsweep_after, 0}]),
  par_connect(Listen).
  
loop(Socket) ->
  case read_section(Socket) of
    {ok, Cmd} -> 
      error_logger:info_msg("got cmd ~p~n", [Cmd]),
      execute_command(Cmd, Socket),
      loop(Socket);
    {error, Reason} ->
      gen_tcp:close(Socket),
      exit({error, Reason})
  end.

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
    {error, Reason} -> {error, Reason}
  end.
  
write_binary(Socket, Bin, Term) ->
  Length = erlang:byte_size(Bin),
  write_length(Socket, Length),
  gen_tcp:send(Socket, Bin),
  gen_tcp:send(Socket, Term).
  
write_length(Socket, Length) ->
  gen_tcp:send(Socket, integer_to_list(Length)),
  gen_tcp:send(Socket, " ").