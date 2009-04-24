-include_lib("eunit/include/eunit.hrl").

basic_rate_test() ->
  {ok, Pid} = rate:start_link(100),
  ?assertEqual(0.0, rate:get_rate(Pid, 100)),
  rate:add_datapoint(Pid, 100, now()),
  ?assertEqual(100.0, rate:get_rate(Pid, 100)),
  rate:close(Pid).
  
limiting_test() ->
  {ok, Pid} = rate:start_link(3000),
  lists:foreach(fun(N) ->
      rate:add_datapoint(Pid, 1, now())
    end, lists:seq(1, 350)),
  ?assertEqual(1, length(rate:get_datapoints(Pid))),
  ?assertEqual(350.0, rate:get_rate(Pid, 3000)),
  rate:close(Pid).
  
time_limiting_test_() ->
  {timeout, 120, ?_test(test_time_limiting())}.
  
test_time_limiting() ->
  {ok, Pid} = rate:start_link(1),
  lists:foreach(fun(N) ->
      rate:add_datapoint(Pid, 1, now())
    end, lists:seq(1, 50)),
  ?assertEqual(1, length(rate:get_datapoints(Pid))),
  timer:sleep(2000),
  lists:foreach(fun(N) ->
      rate:add_datapoint(Pid, 1, now())
    end, lists:seq(1, 50)),
  ?assertEqual(1, length(rate:get_datapoints(Pid))),
  ?assertEqual(50.0, rate:get_rate(Pid, 1)),
  rate:close(Pid).

queue_test() ->
  Q = {[],[{324,1232143984}]},
  NQ = update(1,1232143985, Q),
  ?assertEqual({[{324,1232143984}],[{1,1232143985}]}, NQ).