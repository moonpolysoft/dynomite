-module(t).

-export([start/0, config/1]).

start() ->
    eunit:test(config(src_dir)).


config(src_dir) ->
    Root = filename:dirname(config(test_dir)),
    filename:absname(filename:join([Root, "elibs"]));
config(test_dir) ->
    filename:dirname(?FILE);
config(priv_dir) ->
    Root = config(test_dir),
    filename:absname(filename:join([Root, "log", atom_to_list(node())])).
    
