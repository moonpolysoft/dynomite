-include_lib("eunit.hrl").
-include("config.hrl").



clean_put_listener_test_() ->
    [{setup, local,
      fun () ->
              {ok, Pid} = dynomite_sup:start_link(config()),
              {ok, Socket} = gen_tcp:connect("localhost", 11333, [binary, {active,false},{packet,0}]),
              {Pid, Socket}
      end,
      fun({Pid, Socket}) ->
              close_conn(Socket, Pid)
      end,
      fun({_, Socket}) ->
              [ fun() ->
                        gen_tcp:send(Socket, "put 5 mykey 0  5 value\n"),
                        ?assertEqual({ok, "succ"}, read_section(Socket)),
                        ?assertEqual({ok, "1"}, read_section(Socket))
                end
               ]
      end
     }].
  
put_and_get_test_() ->
    [{setup, local,
      fun () ->
              {ok, Pid} = dynomite_sup:start_link(config()),
              {ok, Socket} = gen_tcp:connect("localhost", 11333, [binary, {active,false},{packet,0}]),
              {Pid, Socket}
      end,
      fun({Pid, Socket}) ->
              close_conn(Socket, Pid)
      end,
      fun({_, Socket}) ->
              [ fun () ->
                        gen_tcp:send(Socket, "put 5 mykey 0  5 value\n"),
                        ?assertEqual({ok, "succ"}, read_section(Socket)),
                        ?assertEqual({ok, "1"}, read_section(Socket)),
                        gen_tcp:send(Socket, "get 5 mykey\n"),
                        ?assertEqual({ok, "succ"}, read_section(Socket)),
                        ?assertEqual({ok, "1"}, read_section(Socket)),
                        Ctx = read_length_data(Socket),
                        ?assertEqual(<<"value">>, read_length_data(Socket))
                end ]
      end
     }].
                
  
put_and_has_key_test_() ->
    [{setup, local,
      fun () ->
              {ok, Pid} = dynomite_sup:start_link(config()),
              {ok, Socket} = gen_tcp:connect("localhost", 11333, [binary, {active,false},{packet,0}]),
              {Pid, Socket}
      end,
      fun({Pid, Socket}) ->
              close_conn(Socket, Pid)
      end,
      fun({_, Socket}) ->
              [ fun () ->
                        gen_tcp:send(Socket, "put 5 mykey 0  5 value\n"),
                        ?assertEqual({ok, "succ"}, read_section(Socket)),
                        ?assertEqual({ok, "1"}, read_section(Socket)),
                        gen_tcp:send(Socket, "has 5 mykey\n"),
                        ?assertEqual({ok, "yes"}, read_section(Socket)),
                        ?assertEqual({ok, "1"}, read_section(Socket)),
                        gen_tcp:send(Socket, "has 5 nokey\n"),
                        ?assertEqual({ok, "no"}, read_section(Socket)),
                        ?assertEqual({ok, "1"}, read_section(Socket))
                end ]
      end
     }].
  
put_and_delete_test_() ->
    [{setup, local,
      fun () ->
              {ok, Pid} = dynomite_sup:start_link(config()),
              {ok, Socket} = gen_tcp:connect("localhost", 11333, [binary, {active,false},{packet,0}]),
              {Pid, Socket}
      end,
      fun({Pid, Socket}) ->
              close_conn(Socket, Pid)
      end,
      fun({_, Socket}) ->
              [ fun () ->
                        gen_tcp:send(Socket, "put 5 mykey 0  5 value\n"),
                        ?assertEqual({ok, "succ"}, read_section(Socket)),
                        ?assertEqual({ok, "1"}, read_section(Socket)),
                        gen_tcp:send(Socket, "del 5 mykey\n"),
                        ?assertEqual({ok, "succ"}, read_section(Socket)),
                        ?assertEqual({ok, "1"}, read_section(Socket)),
                        gen_tcp:send(Socket, "has 5 mykey\n"),
                        ?assertEqual({ok, "no"}, read_section(Socket)),
                        ?assertEqual({ok, "1"}, read_section(Socket))
                end ]
      end
     }].
  
put_with_error_test_() ->
    [{setup, local,
      fun () ->
              {ok, Pid} = dynomite_sup:start_link(fail_config()),
              {ok, Socket} = gen_tcp:connect("localhost", 11333, [binary, {active,false},{packet,0}]),
              {Pid, Socket}
      end,
      fun({Pid, Socket}) ->
              close_conn(Socket, Pid)
      end,
      fun({_, Socket}) ->
              [ fun () ->
                        gen_tcp:send(Socket, "put 5 mykey 0  5 value\n"),
                        ?assertEqual({ok, "fail"}, read_section(Socket)),
                        _ = read_line(Socket),
                        gen_tcp:send(Socket, "has 5 mykey\n"),
                        ?assertEqual({ok, "fail"}, read_section(Socket)),
                        _ = read_line(Socket),
                        gen_tcp:send(Socket, "get 5 mykey\n"),
                        ?assertEqual({ok, "fail"}, read_section(Socket)),
                        _ = read_line(Socket),
                        gen_tcp:send(Socket, "del 5 mykey\n"),
                        ?assertEqual({ok, "fail"}, read_section(Socket)),
                        _ = read_line(Socket)
                end ]
      end
     }].


close_conn(Socket, Pid) ->
    gen_tcp:send(Socket, "close\n"),
    gen_tcp:close(Socket),
    stop(Pid).
  
stop(Pid) ->
    exit(Pid, shutdown),
    receive
        after 
            100 ->
                ok
        end.


read_line(Socket) ->
  read_line([], Socket).

read_line(Cmd, Socket) ->
  case gen_tcp:recv(Socket, 1) of
    {ok, <<"\n">>} -> lists:reverse(Cmd);
    {ok, <<Char>>} -> read_line([Char|Cmd], Socket)
  end.

config() ->
    #config{n=1,
            r=1,
            w=1,
            q=6,
            live=true,
            port=11333,
            directory=priv_dir(),
            storage_mod=dict_storage}.

fail_config() ->
    C = config(),
    C#config{storage_mod=fail_storage}.

priv_dir() ->
    filename:join(t:config(priv_dir), "storage_test").
