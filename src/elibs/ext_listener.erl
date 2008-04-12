-module (ext_listener).
-export ([start/0]).

-ifdef(TEST).
-include("etest/ext_listener_test.erl").
-endif.

start() ->
	{ok, Listen} = gen_tcp:listen(11211, [binary, {active, once}, {packet, 0}]),
	spawn(fun() -> par_connect(Listen) end).
	
par_connect(Listen) ->
	{ok, Socket} = gen_tcp:accept(Listen),
	io:format("got connection~n"),
	spawn(fun() -> par_connect(Listen) end),
	loop(Socket).
	
loop(Socket) ->
	receive
		{tcp, Socket, Bin} ->
			Cmd = parse_command(Bin),
			io:format("got this: ~p~n", [Cmd]),
			loop(Socket)
	end.
	
parse_command(<<"get", $\s, Key/binary>>) -> {get, Key};

parse_command(<<"set", $\s, Data/binary>>) -> 
	{Key, Value} = split_next_space(Data),
	{set, Key, Value}.
	
split_next_space(Binary) ->
	split_next_space([], Binary).
	
split_next_space(Head, <<$\s, Rest/binary>>) ->
	{list_to_binary(lists:reverse(Head)), Rest};	

split_next_space(Head, <<Char:1/binary, Rest/binary>>) ->
	split_next_space([Char|Head], Rest).