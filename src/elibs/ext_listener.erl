-module (ext_listener).
-export ([start/0, parse_command/1]).

start() ->
	{ok, Listen} = gen_tcp:listen(11211, [binary, {active, once}, {packet, 4}]),
	spawn(fun() -> par_connect(Listen) end).
	
par_connect(Listen) ->
	{ok, Socket} = gen_tcp:accept(Listen),
	spawn(fun() -> par_connect(Listen) end),
	loop(Socket).
	
loop(Socket) ->
	receive
		{tcp, Socket, Bin} ->
			Cmd = parse_command(Bin)
	end.
	
parse_command(<<"get", $\s, Key/binary>>) -> {get, Key}. 