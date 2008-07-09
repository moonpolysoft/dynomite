-module (ext_listener).
-export ([start_link/0]).

-ifdef(TEST).
-include("etest/ext_listener_test.erl").
-endif.

start_link() ->
  Pid = spawn(fun init/0),
  link(Pid),
  {ok, Pid}.
  
init() ->
  {ok, Listen} = gen_tcp:listen(11222, [binary, inet6, {active, false}, {packet, 0}]),
  par_connect(Listen).
  
par_connect(Listen) ->
  {ok, Socket} = gen_tcp:accept(Listen),
  error_logger:info_msg("got connection~n"),
  spawn(fun() -> loop(Socket) end),
  par_connect(Listen).
  
loop(Socket) ->
  Cmd = read_section(Socket),
  error_logger:info_msg("received command: ~p~n", [Cmd]),
  execute_command(Cmd, Socket),
  loop(Socket).

execute_command("get", Socket) ->
  Length = read_length(Socket),
  Key = read_data(Socket, Length),
  case mediator:get(Key) of
    {ok, {Context, Values}} -> send_get(Socket, Context, Values);
    {failure, Reason} -> send_failure(Socket, Reason)
  end;
  
execute_command("put", Socket) ->
  Key = read_length_data(Socket),
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
  Key = read_length_data(Socket),
  case mediator:has_key(Key) of
    {ok, {true, N}} -> send_msg(Socket, "yes", N);
    {ok, {false, N}} -> send_msg(Socket, "no", N);
    {failure, Reason} -> send_failure(Socket, Reason)
  end;
  
execute_command("del", Socket) ->
  Key = read_length_data(Socket),
  case mediator:delete(Key) of
    {ok, N} -> send_msg(Socket, "succ", N);
    {failure, Reason} -> send_failure(Socket, Reason)
  end;
  
execute_command("close", Socket) ->
  gen_tcp:send(Socket, "close\n"),
  gen_tcp:close(Socket),
  exit(closed).
  
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
  Blah = read_section(Socket),
  list_to_integer(Blah).

read_section(Socket) ->
  read_section([], Socket).

read_section(Cmd, Socket) ->
  case gen_tcp:recv(Socket, 1) of
    {ok, <<" ">>} -> lists:reverse(Cmd);
    {ok, <<"\n">>} -> lists:reverse(Cmd);
    {ok, <<Char>>} -> read_section([Char|Cmd], Socket)
  end.
  
write_binary(Socket, Bin, Term) ->
  Length = erlang:byte_size(Bin),
  write_length(Socket, Length),
  gen_tcp:send(Socket, Bin),
  gen_tcp:send(Socket, Term).
  
write_length(Socket, Length) ->
  gen_tcp:send(Socket, integer_to_list(Length)),
  gen_tcp:send(Socket, " ").