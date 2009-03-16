#!/usr/bin/env escript
%% -*- erlang -*-
%%! +K true +A 128 +P 60000 -smp enable -sname benchmark -pa ./ebin -pa ./deps/thrift/ebin
-mode(compile).

-record(config, {hosts=[],concurrency=20,ratio=0.5,logdir="bench_log",size=100,keyspace=100000}).

main(Options) ->
  code:add_pathsa(["./ebin", "./deps/thrift/ebin"]),
  process_flag(trap_exit, true),
  case parse_options(Options) of
    {error, Msg} -> usage(Msg);
    Config -> run(Config)
  end,
  receive
    {ok, Val} -> 
      io:format("died with ~p~n", [Val]),
      Val
  end.
  
run(Config = #config{keyspace=Keyspace,size=Size,concurrency=Concurrency,hosts=Hosts}) ->
  Data = generate_test_data(Keyspace, Size),
  spawn_workers(Data, Config, Hosts, Concurrency).
  
spawn_workers(_, _, _, 0) ->
  ok;
  
spawn_workers(Data, Config = #config{hosts=Hosts}, [], N) ->
  spawn_workers(Data, Config, Hosts, N);
  
spawn_workers(Data, Config, [Host|Hosts], N) ->
  spawn_link(fun() ->
      worker(Config, Host, Data)
    end),
  spawn_workers(Data, Config, Hosts, N-1).
  
worker(#config{logdir=Logdir,ratio=Ratio}, Host, Data) ->
  io:format("starting worker for ~p~n", [Host]),
  {Mega, Sec, Micro} = now(),
  random:seed(Mega, Sec, Micro),
  Filename = filename:join(Logdir, pid_to_list(self()) ++ "_" ++ Host ++ ".log"),
  {ok, File} = file:open(Filename, [write, raw]),
  [Hostname, StrPort] = string:tokens(Host, ":"),
  Port = list_to_integer(StrPort),
  {ok, Client} = dynomite_thrift_client:start_link(Hostname, Port),
  worker_loop(Client, File, Data, Ratio, Host).
  
generate_test_data(Keyspace, Size) ->
  A = array:new(Keyspace),
  Keychars = lib_misc:ceiling(math:log(Keyspace) / math:log(26)),
  Key = duplicate(Keychars, $a),
  generate_test_data(Keyspace, Key, Size, A).
  
generate_test_data(0, _, _, A) -> A;
  
generate_test_data(N, Key, Size, A) ->
  generate_test_data(N-1, lib_misc:succ(Key), Size, array:set(N-1, {Key, new_bytes(Size)}, A)).
  
duplicate(N, E) ->
  duplicate(N, E, []).
  
duplicate(0, _, A) -> A;
duplicate(N, E, A) -> duplicate(N-1, E, [E|A]).
  
new_bytes(Size) ->
  << << N:8 >> || N <- lists:map(fun(_) -> random:uniform(256) end, lists:seq(1,Size)) >>.
  
worker_loop(Client, File, Data, Ratio, Host) ->
  {Key,Value} = array:get(random:uniform(array:size(Data))-1, Data),
  Start = lib_misc:now_float(),
  Result = (catch call_server(random:uniform(), Ratio, Client, Key, Value)),
  End = lib_misc:now_float(),
  case Result of
    {ok, Method} -> file:write(File, io_lib:format("~p\t~s\t~p\t~s\t~s~n", [End, Method, End - Start, Key, Host]));
    {'EXIT', _} -> file:write(File, io_lib:format("~p\terror\t~p\t~s\t~s~n", [End, End - Start, Key, Host]))
  end,
  worker_loop(Client, File, Data, Ratio, Host).

call_server(Rand, Ratio, Client, Key, Value) when Rand < Ratio ->
  dynomite_thrift_client:put(Client, Key, undefined, Value),
  {ok, "put"};
  
call_server(_, _, Client, Key, _) ->
  dynomite_thrift_client:get(Client, Key),
  {ok, "get"}.

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
  Config#config{keyspace=list_to_integer(Arg)}.

usage(Msg) ->
  io:format("error: ~p", [Msg]),
  io:format("Usage: distributed_bench [options]~n"),
  io:format("    -h, --host [HOST]                Add another host to test against.  Should add the whole cluster.~n"),
  io:format("    -c, --concurrency [INT]          the concurrency level for the test.  How many clients to start.~n"),
  io:format("    -r, --ratio [R]                  the ratio of gets to puts.  0.0 means all puts, 1.0 means all gets.~n"),
  io:format("    -l, --log [LOGDIR]               Where the instances should log their raw performance data.~n"),
  io:format("    -s, --size [SIZE]                The size of the values to use, in bytes.~n"),
  io:format("    -k, --keyspace [KEYSPACE]        The integer size of the keyspace.~n"),
  halt(1).
