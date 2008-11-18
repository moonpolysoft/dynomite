-include_lib("eunit.hrl").

init_integrated(Good, Bad) ->
    GoodNodes = start_nodes(dict_storage, Good),
    BadNodes = start_nodes(fail_storage, Bad),
    wait(500),
    Nodes = jointo(GoodNodes ++ BadNodes),
    % wait(100),
    Nodes.
  
stop_integrated(Nodes) ->
    [ slave:stop(N) || N <- Nodes ].
  
start_nodes(Module, Num) ->
    start_nodes(Module, Num, []).

start_nodes(_Module, 0, Nodes) ->
    Nodes;
start_nodes(Module, N, Nodes) ->
    Name = list_to_atom(
             "mediator_test_" ++ 
             atom_to_list(Module) ++
             integer_to_list(N)),
    Args = node_args(N),
    Args1 = Args ++ 
        " -dynomite storage_mod " ++ atom_to_list(Module) ++
        " -run dynomite start",
    {ok, Node} = slave:start(localhost, Name, Args1),
    ?debugFmt("Started slave node ~p", [Node]),
    start_nodes(Module, N - 1, [Node|Nodes]).


jointo([N|Others]) ->
    jointo(N, Others, []).

jointo(Main, [], Nodes) ->
    [Main|Nodes];
jointo(Main, [O|Others], Nodes) ->
    rpc:call(O, membership, join_node, [Main, O]),
    jointo(Main, Others, [O|Nodes]).

node_args(N) ->
    node_args(N, [boot, pz, pa, priv_dir], []).

node_args(N, [], Args) ->
    Args ++ 
        " -dynomite port " ++ integer_to_list(11222 + N) ++
        " -dynomite thrift_port " ++ integer_to_list(9200 + N);
node_args(N, [priv_dir|Rest], Args) ->
    {ok, [[Pd]]} = init:get_argument(priv_dir),
    DynDir =  filename:join(Pd, "mediator." ++ uniq(N)),
    filelib:ensure_dir(filename:join(DynDir, "dummy")),
    node_args(N, Rest, Args ++ " -dynomite directory '\"" ++ DynDir ++ "\"'" ++
              " -sasl sasl_error_logger '{file, \"" ++ 
              filename:join(DynDir, "sasl.log") ++ "\"}'" ++
              " -kernel error_logger '{file, \"" ++
              filename:join(DynDir, "kernel.log") ++ "\"}'");
node_args(N, [A|Rest], Args) when A =:= pa orelse A =:= pz ->
    case arg_val(A) of 
        undefined ->
            node_args(N, Rest, Args);
        Val ->
            Arg = string:join(
                    [root_path(P) || P <- Val],
                    " "),
            node_args(N, Rest, Args ++ " -" ++ atom_to_list(A) ++ " " ++ Arg)
    end;
node_args(N, [A|Rest], Args) ->
    case arg_val(A) of
        undefined ->
            node_args(N, Rest, Args);
        Val ->
            Arg = string:join(Val, " "),
            node_args(N, Rest, Args ++ " -" ++ atom_to_list(A) ++ " " ++ Arg)
    end.
            

arg_val(A) ->
    %% init:get_argument returns lists of lists of lists, we don't need that
    %% proplists:get_value returns just a list of lists which is easier
    %% to work with
    case proplists:get_value(A, init:get_arguments()) of
        undefined ->
            undefined;
        Val ->
            Val
    end.

root_path(P) ->
    Root = filename:dirname(t:config(test_dir)),
    filename:absname(filename:join(Root, P)).
        
all_servers_working_test_() ->
    [{setup, local,
      fun() ->
              init_integrated(3, 0)
      end,
      fun(Nodes) ->
              stop_integrated(Nodes)
      end,
      fun([N|_Nodes]) ->
              [?_assertEqual({ok, 2}, 
                             rpc:call(N, mediator, 
                                      put, [<<"key1">>, [], <<"value1">>])),
               ?_assertMatch({ok, {_, [<<"value1">>]}},
                             rpc:call(N, mediator, get, [<<"key1">>])),
               ?_assertEqual({ok, {true, 2}}, 
                             rpc:call(N, mediator, has_key, [<<"key1">>])),
               ?_assertEqual({ok, 2}, 
                             rpc:call(N, mediator, delete, [<<"key1">>])),
               ?_assertEqual({ok, {false, 2}}, 
                             rpc:call(N, mediator, has_key, [<<"key1">>])),
               ?_assertEqual({ok, not_found}, 
                             rpc:call(N, mediator, get, [<<"key1">>]))
              ]
      end      
     }].
  
one_bad_server_test_() ->
    [{setup, local,
      fun() ->
              init_integrated(2, 1)
      end,
      fun(Nodes) ->
              stop_integrated(Nodes)
      end,
      fun([N|_Nodes]) ->
              [?_assertEqual({ok, 2},
                             rpc:call(N, mediator,
                                      put, [<<"key1">>, [], <<"value1">>])),
               ?_assertMatch({ok, {_, [<<"value1">>]}},
                             rpc:call(N, mediator, 
                                      get, [<<"key1">>])),
               ?_assertEqual({ok, {true, 2}},
                             rpc:call(N, mediator, has_key, [<<"key1">>])),
               ?_assertEqual({ok, 2},
                             rpc:call(N, mediator, delete, [<<"key1">>])),
               ?_assertEqual({ok, {false, 2}},
                             rpc:call(N, mediator, has_key, [<<"key1">>])),
               ?_assertEqual({ok, not_found},
                             rpc:call(N, mediator, get, [<<"key1">>]))
              ]
      end
     }].
  
two_bad_servers_test_() ->
    [{setup, local,
      fun() ->
              init_integrated(1, 2)
      end,
      fun(Nodes) ->
              stop_integrated(Nodes)
      end,
      fun([N|_Nodes]) ->
              [?_assertMatch({failure, _},
                             rpc:call(N, mediator, 
                                      put, [<<"key1">>, [], <<"value1">>])),
               ?_assertMatch({failure, _},
                             rpc:call(N, mediator, get, [<<"key1">>])),
               ?_assertMatch({failure, _},
                             rpc:call(N, mediator, delete, [<<"key1">>])),
               ?_assertMatch({failure, _},
                             rpc:call(N, mediator, has_key, [<<"key1">>]))
               ]
      end
     }].
  
three_bad_servers_test_() ->
    [{setup, local,
      fun() ->
              init_integrated(0, 3)
      end,
      fun(Nodes) ->
              stop_integrated(Nodes)
      end,
      fun([N|_Nodes]) ->
              [?_assertMatch({failure, _},
                             rpc:call(N, mediator, 
                                      put, [<<"key1">>, [], <<"value1">>])),
               ?_assertMatch({failure, _},
                             rpc:call(N, mediator, get, [<<"key1">>])),
               ?_assertMatch({failure, _},
                             rpc:call(N, mediator, delete, [<<"key1">>])),
               ?_assertMatch({failure, _},
                             rpc:call(N, mediator, has_key, [<<"key1">>]))
               ]
      end
     }].
               

%% Internal
db_key(Name) ->
    Fpath =  filename:join(
               [t:config(priv_dir), "mediator", atom_to_list(Name)]),
    filelib:ensure_dir(filename:join(Fpath, "dmerkle")),
    Fpath.

wait(Ms) ->
    receive
        after 
            Ms ->
                ok
        end.

uniq(N) ->
    {M, S, Ms} = erlang:now(),
    io_lib:format("~p.~p.~p", [N, S, Ms]).
