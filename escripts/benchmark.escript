#!/usr/bin/env escript
%%! +K true +A 128 +P 60000 -smp enable -sname benchmark -pa ./ebin -pa ./deps/thrift/ebin -setcookie e841d215484685567858aaec4d25af2f
-mode(compile).

-record(config, {hosts=[],concurrency=20,ratio=0.5,logdir="bench_log",size=100,keyspace=100000,method=thrift}).

main(Options) ->
  net_kernel:start([benchmark, shortnames]),
  erlang:set_cookie(node(), e841d215484685567858aaec4d25af2f),
  code:add_pathsa(["./ebin", "./deps/thrift/ebin"]),
  process_flag(trap_exit, true),
  case parse_options(Options) of
    {error, Msg} -> usage(Msg);
    Config -> load_gen:run(Config)
  end,
  receive
    {ok, Val} -> 
      io:format("died with ~p~n", [Val]),
      Val
  end.
  
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Options and what have you

parse_options(Opts) ->
  parse_options(Opts, #config{}).
  
parse_options([], Config) -> Config;

parse_options([Name, Arg | Opts], Config) ->
  parse_options(Opts, set_option(longform(Name), Arg, Config)).
  
longform("-h") -> "--host";
longform("-c") -> "--concurrency";
longform("-r") -> "--ratio";
longform("-l") -> "--log";
longform("-s") -> "--size";
longform("-k") -> "--keyspace";
longform("-m") -> "--method";
longform(N) -> N.
  
set_option("--host", Arg, Config = #config{hosts=Hosts}) ->
  Config#config{hosts=[Arg|Hosts]};
  
set_option("--concurrency", Arg, Config) ->
  Config#config{concurrency=list_to_integer(Arg)};
  
set_option("--ratio", Arg, Config) ->
  Config#config{ratio=list_to_float(Arg)};
  
set_option("--log", Arg, Config) ->
  Config#config{logdir=Arg};
  
set_option("--size", Arg, Config) ->
  Config#config{size=list_to_integer(Arg)};
  
set_option("--keyspace", Arg, Config) ->
  Config#config{keyspace=list_to_integer(Arg)};
  
set_option("--method", Arg, Config) ->
  Config#config{method=list_to_atom(Arg)}.

usage(Msg) ->
  io:format("error: ~p", [Msg]),
  io:format("Usage: distributed_bench [options]~n"),
  io:format("    -h, --host [HOST]                Add another host to test against.  Should add the whole cluster.~n"),
  io:format("    -c, --concurrency [INT]          the concurrency level for the test.  How many clients to start.~n"),
  io:format("    -r, --ratio [R]                  the ratio of gets to puts.  0.0 means all gets, 1.0 means all puts.~n"),
  io:format("    -l, --log [LOGDIR]               Where the instances should log their raw performance data.~n"),
  io:format("    -s, --size [SIZE]                The size of the values to use, in bytes.~n"),
  io:format("    -k, --keyspace [KEYSPACE]        The integer size of the keyspace.~n"),
  io:format("    -m, --method [METHOD]            The method of contacting the server (thrift, rpc).~n"),
  halt(1).
