-include_lib("eunit/include/eunit.hrl").
-include("etest/test.hrl").

singular_startup_sequence_test() ->
  configuration:start_link(#config{n=1,r=1,w=1,q=6,directory=priv_dir()}),
  {ok, _} = mock:mock(replication),
  mock:expects(replication, partners, fun({_, [a], _}) -> true end, []),
  {ok, M} = membership2:start_link(a, [a]),
  State = gen_server:call(M, state),
  ?assertEqual(a, State#state.node),
  ?assertEqual([a], State#state.nodes),
  mock:verify_and_stop(replication),
  membership:stop(M),
  configuration:stop(),
  ?assertMatch({ok, [[a]]}, file:consult(priv_file("a.world"))),
  file:delete(priv_file("a.world")).

-define(NODEA, {a, ["d", "1", "4"]}).
-define(NODEB, {b, ["e", "3", "1"]}).
-define(NODEC, {c, ["f", "1", "2"]}).
-define(NODES, [?NODEA, ?NODEB, ?NODEC]).

multi_startup_sequence_test() ->
  configuration:start_link(#config{n=3,r=1,w=1,q=6,directory=priv_dir()}),
  {ok, _} = mock:mock(replication),
  VersionOne = vector_clock:create(make_ref()),
  Pid1 = make_ref(),
  VersionTwo = vector_clock:create(make_ref()),
  Pid2 = make_ref(),
  mock:expects(replication, partners, fun({_, ?NODES, _}) -> true end, [?NODEB, ?NODEC]),
  {ok, _} = stub:stub(membership2, call_join, fun(?NODEB, ?NODEA) ->
      {VersionOne, ?NODES, [{1,Pid1}]};
    (?NODEC, ?NODEA) ->
      {VersionTwo, ?NODES, [{2,Pid2}]}
    end, 2),
  ?debugMsg("proxied"),
  ?debugFmt("check process code: ~p", [erlang:check_process_code(self(), membership2)]),
  {ok, M} = membership2:start_link(?NODEA, ?NODES),
  State = gen_server:call(M, state),
  ?assertEqual(?NODEA, State#state.node),
  ?assertEqual(?NODES, State#state.nodes),
  Servers = State#state.servers,
  ?assertMatch([{1,Pid1},{2,Pid2}], servers_to_list(Servers)),
  ?assertEqual(greater, vector_clock:compare(State#state.version, VersionOne)),
  ?assertEqual(greater, vector_clock:compare(State#state.version, VersionTwo)),
  mock:verify(replication),
  membership:stop(M),
  configuration:stop(),
  ?assertMatch({ok, [?NODES]}, file:consult(priv_file("a.world"))),
  file:delete(priv_file("a.world")).

