-include_lib("eunit.hrl").

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
  ?assertEqual(300, length(rate:get_datapoints(Pid))),
  rate:close(Pid).
  
time_limiting_test_() ->
  {timeout, 120, [{?LINE, fun() ->
    {ok, Pid} = rate:start_link(1),
    lists:foreach(fun(N) ->
        rate:add_datapoint(Pid, 1, now())
      end, lists:seq(1, 50)),
    timer:sleep(2000),
    lists:foreach(fun(N) ->
        rate:add_datapoint(Pid, 1, now())
      end, lists:seq(1, 50)),
    ?assertEqual(50, length(rate:get_datapoints(Pid))),
    rate:close(Pid)
  end}]}.