%%%-------------------------------------------------------------------
%%% File:      load_gen.erl
%%% @author    Cliff Moon <> []
%%% @copyright 2009 Cliff Moon
%%% @doc  
%%%
%%% @end  
%%%
%%% @since 2009-04-06 by Cliff Moon
%%%-------------------------------------------------------------------
-module(load_gen).
-author('').

%% API
-export([run/1, run/7]).

-record(config, {hosts=[],concurrency=20,ratio=0.5,logdir="bench_log",size=100,keyspace=100000,method=thrift}).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% @spec 
%% @doc
%% @end 
%%--------------------------------------------------------------------
run(Hosts, Concurrency, Ratio, Logdir, Size, Keyspace, Method) ->
  run(#config{hosts=Hosts, concurrency=Concurrency,ratio=Ratio,logdir=Logdir,size=Size,keyspace=Keyspace,method=Method}).

run(Config = #config{keyspace=Keyspace,size=Size,concurrency=Concurrency,hosts=Hosts}) ->
  io:format("config: ~p~n", [Config]),
  Data = generate_test_data(Keyspace, Size),
  spawn_workers(Data, Config, Hosts, Concurrency).
  
%%====================================================================
%% Internal functions
%%====================================================================
  
spawn_workers(_, _, _, 0) ->
  ok;
  
spawn_workers(Data, Config = #config{hosts=Hosts}, [], N) ->
  spawn_workers(Data, Config, Hosts, N);
  
spawn_workers(Data, Config, [Host|Hosts], N) ->
  spawn_link(fun() ->
      worker(Config, Host, Data)
    end),
  spawn_workers(Data, Config, Hosts, N-1).
  
worker(#config{logdir=Logdir,ratio=Ratio,method=Method,keyspace=Keyspace}, Host, Data) ->
  io:format("starting worker for ~p~n", [Host]),
  {Mega, Sec, Micro} = now(),
  random:seed(Mega, Sec, Micro),
  Filename = filename:join(Logdir, pid_to_list(self()) ++ "_" ++ Host ++ ".log"),
  {ok, File} = file:open(Filename, [write, raw, delayed_write]),
  [Hostname, StrPort] = string:tokens(Host, ":"),
  Port = list_to_integer(StrPort),
  {ok, Client} = case Method of
    thrift -> dynomite_thrift_client:start_link(Hostname, Port);
    rpc -> 
      Nodename = list_to_atom("dynomite@" ++ Hostname),
      case net_adm:ping(Nodename) of
        pong -> ok;
        pang -> io:format("could not connect to ~p~n", [Nodename]), halt(1)
      end,
      {ok, Nodename};
    local -> {ok, list_to_atom(Host)}
  end,
  worker_loop(Method, Client, File, Data, Ratio, Host, Keyspace).
  
generate_test_data(Keyspace, Size) ->
  A = ets:new(test_data, [public]),
  Keychars = lib_misc:ceiling(math:log(Keyspace) / math:log(26)),
  Key = duplicate(Keychars, $a),
  generate_test_data(Keyspace, Key, Size, A).
  
generate_test_data(0, _, _, A) -> A;
  
generate_test_data(N, Key, Size, A) ->
  ets:insert(A, {N, Key, new_bytes(Size)}),
  generate_test_data(N-1, lib_misc:succ(Key), Size, A).
  
duplicate(N, E) ->
  duplicate(N, E, []).
  
duplicate(0, _, A) -> A;
duplicate(N, E, A) -> duplicate(N-1, E, [E|A]).
  
new_bytes(Size) ->
  << << N:8 >> || N <- lists:map(fun(_) -> random:uniform(256) end, lists:seq(1,Size)) >>.
  
worker_loop(Method, Client, File, Data, Ratio, Host, Keyspace) ->
  [{_,Key,Value}] = ets:lookup(Data, random:uniform(Keyspace)),
  Start = lib_misc:now_float(),
  Result = (catch call_server(Method, random:uniform(), Ratio, Client, Key, Value)),
  End = lib_misc:now_float(),
  case Result of
    {ok, Type} -> file:write(File, io_lib:format("~p\t~s\t~p\t~s\t~s~n", [End, Type, End - Start, Key, Host]));
    {'failure', _Reason} -> io:format("failure: ~p~n", [_Reason]), exit(_Reason);
    {'EXIT', _} -> file:write(File, io_lib:format("~p\terror\t~p\t~s\t~s~n", [End, End - Start, Key, Host]))
  end,
  worker_loop(Method, Client, File, Data, Ratio, Host, Keyspace).

call_server(local, Rand, Ratio, _, Key, Value) when Rand < Ratio ->
  case mediator:put(Key, {self(), undefined}, Value) of
    {ok, _} -> {ok, "put"};
    Res -> {'EXIT', Res}
  end;

call_server(local, _, _, _, Key, _) ->
  case mediator:get(Key) of
    {ok, _} -> {ok, "get"};
    Res -> {'EXIT', Res}
  end;

call_server(rpc, Rand, Ratio, Host, Key, Value) when Rand < Ratio ->
  case rpc:call(Host, mediator, put, [Key, {self(), undefined}, Value]) of
    {badrpc, Reason} -> {'EXIT', Reason};
    Res -> {ok, "put"}
  end;
  
call_server(rpc, _, _, Host, Key, _) ->
  case rpc:call(Host, mediator, get, [Key]) of
    {badrpc, Reason} -> {'EXIT', Reason};
    Res -> {ok, "get"}
  end;

call_server(thrift, Rand, Ratio, Client, Key, Value) when Rand < Ratio ->
  dynomite_thrift_client:put(Client, Key, undefined, Value),
  {ok, "put"};
  
call_server(thrift, _, _, Client, Key, _) ->
  dynomite_thrift_client:get(Client, Key),
  {ok, "get"}.

