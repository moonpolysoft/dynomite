-module(t).

-export([start/0, config/1]).

start() ->
    notimplemented.


config(test_dir) ->
    Root = filename:dirname(?FILE);
config(priv_dir) ->
    Root = config(test_dir),
    filename:absname(filename:join([Root, "log", atom_to_list(node())])).
    
