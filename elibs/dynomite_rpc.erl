%% An interface to dynomite

-module(dynomite_rpc).
-author('cliff@powerset.com').

-export([connect/1, get/2, put/4, has_key/2, delete/2, close/1]).

connect(Node) ->
  case net_adm:ping(Node) of
    pong ->
      {ok, Node};
    pang -> {error, "Cannot connect."}
  end.
  
get(Node, Key) ->
  GetFun = fun(N) ->
    case rpc:call(N, mediator, get, [Key]) of
      {badrpc, Reason} -> {failure, Reason};
      Result -> Result
    end
  end,
  robustify(Node, GetFun).
  
put(Node, Key, Context, Value) ->
  PutFun = fun(N) ->
    case rpc:call(N, mediator, put, [Key, Context, Value]) of
      {badrpc, Reason} -> {failure, Reason};
      Result -> Result
    end
  end,
  robustify(Node, PutFun).
  
has_key(Node, Key) ->
  HasFun = fun(N) ->
    case rpc:call(N, mediator, has_key, [Key]) of
      {badrpc, Reason} -> {failure, Reason};
      Result -> Result
    end
  end,
  robustify(Node, HasFun).
  
delete(Node, Key) ->
  DelFun = fun(N) ->
    case rpc:call(N, mediator, delete, [Key]) of
      {badrpc, Reason} -> {failure, Reason};
      Result -> Result
    end
  end,
  robustify(Node, DelFun).
  
close(Node) -> erlang:disconnect(Node).


robustify(Node, Fun) ->
  erlang:monitor_node(Node, true),
  R = receive
    {nodedown, Node} ->
      % io:format("node ~p was down~n", [Node]),
      case dynomite:running_nodes() of
        [] -> {failure, "No dynomite nodes available."};
        [NextNode|_] -> Fun(NextNode)
      end
  after 0 ->
    Fun(Node)
  end,
  erlang:monitor_node(Node, false),
  R.