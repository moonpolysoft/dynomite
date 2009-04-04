%% An interface to dynomite using 

-module(dynomite_rpc).
-author('cliff@powerset.com').

-export([connect/1, get/2, put/4, has_key/2, delete/2, close/1]).

connect(Node) ->
  case net_adm:ping(Node) of
    pong -> {ok, Node};
    pang -> {error, "Cannot connect."}
  end.
  
get(Node, Key) ->
  case rpc:call(Node, mediator, get, [Key]) of
    {badrpc, Reason} -> {failure, Reason};
    Result -> Result
  end.
  
put(Node, Key, Context, Value) ->
  case rpc:call(Node, mediator, put, [Key, Context, Value]) of
    {badrpc, Reason} -> {failure, Reason};
    Result -> Result
  end.
  
has_key(Node, Key) ->
  case rpc:call(Node, mediator, has_key, [Key]) of
    {badrpc, Reason} -> {failure, Reason};
    Result -> Result
  end.
  
delete(Node, Key) ->
  case rpc:call(Node, mediator, delete, [Key]) of
    {badrpc, Reason} -> {failure, Reason};
    Result -> Result
  end.
  
close(Node) -> erlang:disconnect(Node).