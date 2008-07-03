-include_lib("include/eunit/eunit.hrl").

start_server_test() ->
  {ok, Pid} = mediator:start_link(3).