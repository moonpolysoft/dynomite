-include_lib("eunit/include/eunit.hrl").
 

init_integrated(Good, Bad, Config) ->
  process_flag(trap_exit, true),
  {ok, _} = configuration:start_link(Config),
  GoodServers = start_storage_servers(dict_storage, good_store, Good, []),
  BadServers = start_storage_servers(fail_storage, bad_store, Bad, []),
  {ok, MockMem} = mock_genserver:start_link({local, membership}),
  mock_genserver:expects_call(MockMem, {servers_for_key, unbound}, fun(_, _) -> GoodServers ++ BadServers end).
  
stop_integrated(Good, Bad) ->
  ok = stop_storage_servers(bad_store, Bad),
  ok = stop_storage_servers(good_store, Good),
  configuration:stop(),
  receive {'EXIT', Pid, Val} -> ok end,
  mock_genserver:stop(membership).
  
start_storage_servers(_Module, _Name, 0, Servers) -> Servers;
start_storage_servers(Module, Name, N, Servers) ->
  {ok, Pid} = storage_server:start_link(Module, ok, list_to_atom(lists:concat([Name, N])), 0, 0, undefined),
  start_storage_servers(Module, Name, N-1, [Pid|Servers]).
  
stop_storage_servers(_Name, 0) -> ok;
stop_storage_servers(Name, N) ->
  storage_server:close(list_to_atom(lists:concat([Name, N]))),
  receive {'EXIT', Pid, Val} -> ok end,
  stop_storage_servers(Name, N-1).
  
all_servers_working_test() ->
  init_integrated(3, 0, #config{n=3,r=2,w=2,q=1}),
  % we get back 2 because we bank out before the 3rd can come back
  ?assertEqual({ok, 2}, mediator:put(<<"key1">>, [], <<"value1">>)),
  {ok, {_, [<<"value1">>]}} = mediator:get(<<"key1">>),
  ?assertEqual({ok, {true, 2}}, mediator:has_key(<<"key1">>)),
  ?assertEqual({ok, 2}, mediator:delete(<<"key1">>)),
  ?assertEqual({ok, {false, 2}}, mediator:has_key(<<"key1">>)),
  ?assertEqual({ok, not_found}, mediator:get(<<"key1">>)),
  stop_integrated(3, 0).
  
one_bad_server_test() ->
  init_integrated(2, 1, #config{n=3,r=2,w=2,q=1}),
  ?assertEqual({ok, 2}, mediator:put(<<"key1">>, [], <<"value1">>)),
  {ok, {_Context, [<<"value1">>]}} = mediator:get(<<"key1">>),
  ?assertEqual({ok, {true, 2}}, mediator:has_key(<<"key1">>)),
  ?assertEqual({ok, 2}, mediator:delete(<<"key1">>)),
  ?assertEqual({ok, {false, 2}}, mediator:has_key(<<"key1">>)),
  ?assertEqual({ok, not_found}, mediator:get(<<"key1">>)),
  stop_integrated(2, 1).
  
two_bad_servers_test() ->
  init_integrated(1, 2, #config{n=3,r=2,w=2,q=1}),
  {failure, _} = mediator:put(<<"key1">>, [], <<"value1">>),
  {failure, _} = mediator:get(<<"key1">>),
  {failure, _} = mediator:delete(<<"key1">>),
  {failure, _} = mediator:has_key(<<"key1">>),
  stop_integrated(1, 2).
  
three_bad_servers_test() ->
  init_integrated(0, 3, #config{n=3,r=2,w=2,q=1}),
  {failure, _} = mediator:put(<<"key1">>, [], <<"value1">>),
  {failure, _} = mediator:get(<<"key1">>),
  {failure, _} = mediator:delete(<<"key1">>),
  {failure, _} = mediator:has_key(<<"key1">>),
  stop_integrated(0, 3).