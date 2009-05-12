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
  ?assertMatch({ok, [[a]]}, file:consult(filename:join([priv_dir(), "a.world"]))).

multi_startup_sequence_test() ->
  configuration:start_link(#config{n=3,r=1,w=1,q=6,directory=priv_dir()}),
  {ok, _} = mock:mock(replication),
  VersionOne = vector_clock:create(make_ref()),
  Pid1 = make_ref(),
  VersionTwo = vector_clock:create(make_ref()),
  Pid2 = make_ref(),
  mock:expects(replication, partners, fun({_, [a,b,c], _}) -> true end, [b, c]),
  {ok, _} = stub:stub(membership2, call_join, fun(b, a) ->
      {VersionOne, [a,b,c], [{1,Pid1}]};
    (c, a) ->
      {VersionTwo, [a,b,c], [{2,Pid2}]}
    end, 2),
  ?debugMsg("proxied"),
  ?debugFmt("check process code: ~p", [erlang:check_process_code(self(), membership2)]),
  {ok, M} = membership2:start_link(a, [a,b,c]),
  State = gen_server:call(M, state),
  ?assertEqual(a, State#state.node),
  ?assertEqual([a,b,c], State#state.nodes),
  Servers = State#state.servers,
  ?assertMatch([{1,Pid1},{2,Pid2}], servers_to_list(Servers)),
  ?assertEqual(greater, vector_clock:compare(State#state.version, VersionOne)),
  ?assertEqual(greater, vector_clock:compare(State#state.version, VersionTwo)),
  mock:verify(replication),
  membership:stop(M),
  configuration:stop(),
  ?assertMatch({ok, [[a,b,c]]}, file:consult(filename:join([priv_dir(), "a.world"]))).
