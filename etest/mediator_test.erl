-include_lib("eunit.hrl").

init_integrated(Good, Bad, Config) ->
    {ok, _} = configuration:start_link(Config),
    {ok, _} = membership:start_link(Config),
    {ok, _} = mediator:start_link(Config),
    ok = start_storage_servers(dict_storage, good_store, Good),
    ok = start_storage_servers(fail_storage, bad_store, Bad).
  
stop_integrated(Good, Bad) ->
  ok = stop_storage_servers(bad_store, Bad),
  ok = stop_storage_servers(good_store, Good),
  mediator:stop(),
  membership:stop().
  
start_storage_servers(_Module, _Name, 0) -> ok;
start_storage_servers(Module, Name, N) ->
  {ok, _} = storage_server:start_link(Module, db_key(Name), list_to_atom(lists:concat([Name, N])), 0, (2 bsl 31), 4096),
  start_storage_servers(Module, Name, N-1).
  
stop_storage_servers(_Name, 0) -> ok;
stop_storage_servers(Name, N) ->
  storage_server:close(list_to_atom(lists:concat([Name, N]))),
  stop_storage_servers(Name, N-1).
  
all_servers_working_test() ->
  init_integrated(3, 0, #config{q=10}),
  ?assertEqual({ok, 3}, mediator:put(<<"key1">>, [], <<"value1">>)),
  ?_test({ok, {_Context, [<<"value1">>]}} = mediator:get(<<"key1">>)),
  ?assertEqual({ok, {true, 3}}, mediator:has_key(<<"key1">>)),
  ?assertEqual({ok, 3}, mediator:delete(<<"key1">>)),
  ?assertEqual({ok, {false, 3}}, mediator:has_key(<<"key1">>)),
  ?assertEqual({ok, not_found}, mediator:get(<<"key1">>)),
  stop_integrated(3, 0).
  
one_bad_server_test() ->
  init_integrated(2, 1, {2, 2, 3}),
  {ok, 2} = mediator:put(<<"key1">>, [], <<"value1">>),
  {ok, {_Context, [<<"value1">>]}} = mediator:get(<<"key1">>),
  {ok, {true, 2}} = mediator:has_key(<<"key1">>),
  {ok, 2} = mediator:delete(<<"key1">>),
  {ok, {false, 2}} = mediator:has_key(<<"key1">>),
  {ok, not_found} = mediator:get(<<"key1">>),
  stop_integrated(2, 1).
  
two_bad_servers_test() ->
  init_integrated(1, 2, {2, 2, 3}),
  {failure, _} = mediator:put(<<"key1">>, [], <<"value1">>),
  {failure, _} = mediator:get(<<"key1">>),
  {failure, _} = mediator:delete(<<"key1">>),
  {failure, _} = mediator:has_key(<<"key1">>),
  stop_integrated(1, 2).
  
three_bad_servers_test() ->
  init_integrated(0, 3, {2, 2, 3}),
  {failure, _} = mediator:put(<<"key1">>, [], <<"value1">>),
  {failure, _} = mediator:get(<<"key1">>),
  {failure, _} = mediator:delete(<<"key1">>),
  {failure, _} = mediator:has_key(<<"key1">>),
  stop_integrated(0, 3).

%% Internal
db_key(Name) ->
    Fpath =  filename:join(
               [t:config(priv_dir), "mediator", atom_to_list(Name)]),
    filelib:ensure_dir(filename:join(Fpath, "dmerkle")),
    Fpath.
