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
  mock:verify(replication),
  membership:stop(M),
  configuration:stop(),
  ?assertMatch({ok, [[a]]}, file:consult(filename:join([priv_dir(), "a.world"]))).

multi_startup_sequence_test() ->
  
