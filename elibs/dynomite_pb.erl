-module(dynomite_pb).

-export([start/0, start/0, dispatch/1]).

-include("config.hrl").
-include("common.hrl").
-include("dynomite_types.hrl").

%%%%% EXTERNAL INTERFACE %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
start() ->
  Config = configuration:get_config(),
  mochiweb_http:start([{name, ?MODULE}, {loop, fun dispatch/1}, {port, Config#config.pb_port}]).

stop() ->
    mochiweb_http:stop(?MODULE).

dispatch(Req) ->
    case [Req:get(method), Req:get(path)] of
        ['GET', "/" ++ Key] ->
            case mediator:get(Key) of
                {ok, not_found} ->
                    Req:not_found();
                {ok, {Context, Values}} ->
                    Msg = erlang:iolist_to_binary([
                        protobuffs:encode(1, Key, string),
                        protobuffs:encode(2, erlang:term_to_binary(Context), bytes),
                        protobuffs:encode(3, Values, bytes)
                    ]),
                    Req:respond({201, [{"content-type", "application/x-protobuffs"}], Msg});
                {failure, Error} ->
                    Req:respond({500, [], <<>>})
            end;
        ['PUT', _] ->
            case protobuffs:decode_many(Req:recv_body()) of
                [{1, Key}, {2, Context}, {3, Values}] -> 
                    case mediator:put(binary_to_list(Key), {self(), erlang:binary_to_term(Context)}, Values) of
                        {ok, _} ->
                            Req:respond({201, [], <<>>});
                        _ ->
                            Req:respond({500, [], <<>>})
                    end;
                _ ->
                    Req:respond({500, [], <<>>})
            end;
        ['HEAD', "/" ++ Key] ->
            case mediator:has_key(Key) of
                {ok, {Bool, N}} when is_boolean(Bool), N > 0 ->
                    Req:respond({200, [], <<"">>});
                _ ->
                    Req:not_found()
            end;
        ['DELETE', "/" ++ Key] ->
            case mediator:delete(Key) of
                {ok, _} ->
                    Req:respond({200, [], <<"">>});
                _ ->
                    Req:not_found()
            end;
        Other ->
            Req:not_found()
    end.
