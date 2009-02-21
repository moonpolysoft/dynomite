%% Copyright (c) 2009 Nick Gerakines <nick@gerakines.net>
%% 
%% Permission is hereby granted, free of charge, to any person
%% obtaining a copy of this software and associated documentation
%% files (the "Software"), to deal in the Software without
%% restriction, including without limitation the rights to use,
%% copy, modify, merge, publish, distribute, sublicense, and/or sell
%% copies of the Software, and to permit persons to whom the
%% Software is furnished to do so, subject to the following
%% conditions:
%% 
%% The above copyright notice and this permission notice shall be
%% included in all copies or substantial portions of the Software.
%% 
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
%% EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
%% OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
%% NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
%% HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
%% WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
%% FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
%% OTHER DEALINGS IN THE SOFTWARE.
%% 
%% @author Nick Gerakines <nick@gerakines.net> [http://socklabs.com/]
%% @doc A RESTful Protocol Buffers interface using MochiWeb. This interface 
%% provides very basic CRUD functionality by exposing the GET, PUT, HEAD and
%% DELETE HTTP methods along with a URI scheme that associates URIs with key
%% value pairs stored in Dynomite.
%% 
%% This module is open source and available under the terms of the MIT
%% license.
%% 
%% The protocol buffers message used to represent a dynmoite mesage is fairly
%% simple:
%% 
%% message Dynomite_Message {
%%     required string key = 1;
%%     required bytes context = 2;
%%     required bytes value = 3;
%% }
%% 
%% The following is an example use of this interface.
%% 
%% 1> Data = erlang:iolist_to_binary([
%%     protobuffs:encode(1, "foo", string),
%%     protobuffs:encode(2, erlang:term_to_binary([1]), bytes),
%%     protobuffs:encode(3, <<"hello world">>, bytes)
%% ]).
%% 2> http:request(put, {"http://127.0.0.1:9300/", [], "application/x-protobuffs", Data}, [], []).
%% 3> http:request(get, {"http://127.0.0.1:9300/foo", []}, [], []).
%% 
-module(dynomite_pb).
-author('Nick Gerakines <nick@gerakines.net>').
-export([start_link/0, stop/0, dispatch/1]).
-include("config.hrl").


%% @doc Attempts to start the protocol buffers interface. This is dependant on
%% the #config.pb_port record field being set.
start_link() ->
    Config = configuration:get_config(),
    case Config#config.pb_port of
        undefined ->
            dummy_server:start_link(?MODULE);
        Port ->
            mochiweb_http:start([{name, ?MODULE}, {loop, fun dispatch/1}, {port, Port}])
    end.

%% @doc Attempts to stop the protocol buffers interface.
stop() ->
    catch mochiweb_http:stop(?MODULE).

%% @doc Process requests as dispatched by the MochiWeb application. Request
%% processing is based on the HTTP method and URI as per mochiweb_request:get(method)
%% and mochiweb_request:get(path) function calls. The GET, PUT, HEAD and DELETE
%% HTTP methods are supported and map to the retrieval, creation, existance
%% checking and removal of key/value pairs within Dynomite.
%% 
%% The GET, HEAD and DELETE request types all use the URI to indicate the key
%% that the request is associated with. In all cases, the key is considered
%% any characters after the root "/" of the URI. For example, the URI "/foobar"
%% indicates that the key is "foobar".
%% 
%% In PUT requests, the uri is simple "/" and the key is contained in the
%% protocol buffers encoded body.
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
                    Req:respond({200, [{"content-type", "application/x-protobuffs"}], Msg});
                {failure, Error} ->
                    Req:respond({500, [], <<>>})
            end;
        ['PUT', "/"] ->
            case protobuffs:decode_many(Req:recv_body()) of
                [{1, Key}, {2, Context}, {3, Values}] -> 
                    case mediator:put(binary_to_list(Key), {self(), erlang:binary_to_term(Context)}, Values) of
                        {ok, _} ->
                            Req:respond({201, [], <<>>});
                        _ ->
                            Req:respond({500, [], <<>>})
                    end;
                _ ->
                    Req:respond({422, [], <<>>})
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
        [_, _] ->
            Req:respond({405, [], <<"">>})
    end.
