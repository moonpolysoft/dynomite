-module(t).

-include_lib("eunit.hrl").
-export([start/0, config/1]).

start() ->
    eunit:test(config(src_dir)).


config(src_dir) ->
    Root = filename:dirname(config(test_dir)),
    filename:absname(filename:join([Root, "elibs"]));
config(test_dir) ->
    filename:dirname(?FILE);
config(priv_dir) ->
    case init:get_argument(priv_dir) of
        {ok, [[Dir]]} ->
            Dir;
        Other ->
            ?debugFmt("priv_dir argument result: ~p", [Other]),
            Root = config(test_dir),
            filename:absname(
              filename:join([Root, "log", atom_to_list(node())]))
    end.
    
