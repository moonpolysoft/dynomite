-include_lib("eunit.hrl").

clean_put_listener_test() ->
  {ok, Pid} = dynomite_sup:start_link({{1,1,1},[{fs_storage, "/Users/cliff/data/storage_test", fsstore}]}),
  {ok, Socket} = gen_tcp:connect("localhost", 11222, [binary, {active,false},{packet,0}]),
  gen_tcp:send(Socket, "put 5 mykey 0  5 value\n"),
  "succ" = read_section(Socket),
  "1" = read_section(Socket),
  close_conn(Socket, Pid).
  
put_and_get_test() ->
  {ok, Pid} = dynomite_sup:start_link({{1,1,1},[{fs_storage, "/Users/cliff/data/storage_test", fsstore}]}),
  {ok, Socket} = gen_tcp:connect("localhost", 11222, [binary, {active,false},{packet,0}]),
  gen_tcp:send(Socket, "put 5 mykey 0  5 value\n"),
  "succ" = read_section(Socket),
  "1" = read_section(Socket),
  gen_tcp:send(Socket, "get 5 mykey\n"),
  "succ" = read_section(Socket),
  "1" = read_section(Socket),
  Ctx = read_length_data(Socket),
  <<"value">> = read_length_data(Socket),
  close_conn(Socket, Pid).
  
put_and_has_key_test() ->
  {ok, Pid} = dynomite_sup:start_link({{1,1,1},[{fs_storage, "/Users/cliff/data/storage_test", fsstore}]}),
  {ok, Socket} = gen_tcp:connect("localhost", 11222, [binary, {active,false},{packet,0}]),
  gen_tcp:send(Socket, "put 5 mykey 0  5 value\n"),
  "succ" = read_section(Socket),
  "1" = read_section(Socket),
  gen_tcp:send(Socket, "has 5 mykey\n"),
  "yes" = read_section(Socket),
  "1" = read_section(Socket),
  gen_tcp:send(Socket, "has 5 nokey\n"),
  "no" = read_section(Socket),
  "1" = read_section(Socket),
  close_conn(Socket, Pid).
  
put_and_delete_test() ->
  {ok, Pid} = dynomite_sup:start_link({{1,1,1},[{fs_storage, "/Users/cliff/data/storage_test", fsstore}]}),
  {ok, Socket} = gen_tcp:connect("localhost", 11222, [binary, {active,false},{packet,0}]),
  gen_tcp:send(Socket, "put 5 mykey 0  5 value\n"),
  "succ" = read_section(Socket),
  "1" = read_section(Socket),
  gen_tcp:send(Socket, "del 5 mykey\n"),
  "succ" = read_section(Socket),
  "1" = read_section(Socket),
  gen_tcp:send(Socket, "has 5 mykey\n"),
  "no" = read_section(Socket),
  "1" = read_section(Socket),
  close_conn(Socket, Pid).
  
put_with_error_test() ->
  {ok, Pid} = dynomite_sup:start_link({{1,1,2},[{fail_storage, blah, failstore1},{fail_storage, blah, failstore2},{fail_storage, blah, failstore3}]}),
  {ok, Socket} = gen_tcp:connect("localhost", 11222, [binary, {active,false},{packet,0}]),
  gen_tcp:send(Socket, "put 5 mykey 0  5 value\n"),
  "fail" = read_section(Socket),
  _ = read_line(Socket),
  gen_tcp:send(Socket, "has 5 mykey\n"),
  "fail" = read_section(Socket),
  _ = read_line(Socket),
  gen_tcp:send(Socket, "get 5 mykey\n"),
  "fail" = read_section(Socket),
  _ = read_line(Socket),
  gen_tcp:send(Socket, "del 5 mykey\n"),
  "fail"= read_section(Socket),
  _ = read_line(Socket),
  close_conn(Socket, Pid).
    
close_conn(Socket, Pid) ->
  gen_tcp:send(Socket, "close\n"),
  gen_tcp:close(Socket),
  exit(Pid, shutdown),
  receive
    _ -> true
  end.
  
read_line(Socket) ->
  read_line([], Socket).

read_line(Cmd, Socket) ->
  case gen_tcp:recv(Socket, 1) of
    {ok, <<"\n">>} -> lists:reverse(Cmd);
    {ok, <<Char>>} -> read_line([Char|Cmd], Socket)
  end.