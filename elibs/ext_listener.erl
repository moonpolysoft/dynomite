-module (ext_listener).
-export ([start/0]).

-ifdef(TEST).
-include("etest/ext_listener_test.erl").
-endif.

start() ->
  {ok, Listen} = gen_tcp:listen(11211, [binary, {active, false}, {packet, 0}]),
  spawn(fun() -> par_connect(Listen) end).
  
par_connect(Listen) ->
  {ok, Socket} = gen_tcp:accept(Listen),
  io:format("got connection~n"),
  spawn(fun() -> par_connect(Listen) end),
  loop(Socket).
  
loop(Socket) ->
  Cmd = read_section(Socket),
  io:format("received command: ~p~n", [Cmd]),
  execute_command(Cmd, Socket),
  loop(Socket).

execute_command("get", Socket) ->
  Length = read_length(Socket),
  Key = read_data(Socket, Length),
  case mediator:get(Key) of
    {failure, Reason} -> send_failure(Socket, Reason);
    {Context, Values} -> send_get(Socket, Context, Values)
  end.
  
send_failure(Socket, Reason) ->
  gen_tcp:send(Socket, "fail "),
  gen_tcp:send(Socket, Reason),
  gen_tcp:send(Socket, $\n).
  
send_get(Socket, Context, Values) ->
  ContextBin = term_to_binary(Context),
  write_binary(Socket, ContextBin, $\s),
  ItemLength = length(Values),
  lists:foldl(fun(Value, Acc) ->
    if
      Acc == ItemLength-1 -> write_binary(Socket, Value, $\n);
      true -> write_binary(Socket, Value, $\s)
    end,
    Acc+1
  end, 0, Values).
  
read_data(Socket, Length) ->
  Data = gen_tcp:read(Socket, Length),
  _Term = gen_tcp:read(Socket, 1),
  Data.
  
read_length(Socket) ->
  list_to_integer(read_section(Socket)).

read_section(Socket) ->
  read_section([], Socket).

read_section(Cmd, Socket) ->
  case gen_tcp:recv(Socket, 1) of
    {ok, <<$\s>>} -> lists:reverse(Cmd);
    {ok, <<Char>>} -> [Char, Cmd]
  end.
  
write_binary(Socket, Bin, Term) ->
  Length = erlang:byte_size(Bin),
  write_length(Socket, Length),
  gen_tcp:send(Socket, Bin),
  gen_tcp:send(Socket, Term).
  
write_length(Socket, Length) ->
  gen_tcp:send(Socket, integer_to_list(Length)),
  gen_tcp:send(Socket, $\s).