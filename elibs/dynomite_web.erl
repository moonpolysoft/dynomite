%%%-------------------------------------------------------------------
%%% File:      dynomite_web.erl
%%% @author    Cliff Moon <cliff@powerset.com> []
%%% @copyright 2008 Cliff Moon
%%% @doc  
%%%
%%% @end  
%%%
%%% @since 2008-08-12 by Cliff Moon
%%%-------------------------------------------------------------------
-module(dynomite_web).
-author('cliff@powerset.com').

%% API
-export([start_link/0, stop/0, loop/2]).

-include("config.hrl").
-include("common.hrl").


%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% @spec 
%% @doc
%% @end 
%%--------------------------------------------------------------------

start_link() ->
  Config = configuration:get_config(),
  WebDir = find_web_dir(),
  case Config#config.web_port of
    undefined -> dummy_server:start_link(dynomite_web);
    Port ->
      Loop = fun(Req) ->
          dynomite_web:loop(Req, WebDir)
        end,
      mochiweb_http:start([{name, ?MODULE}, {loop, Loop}, {port, Port}])
  end.

stop() ->
    mochiweb_http:stop(?MODULE).

loop(Req, DocRoot) ->
  "/" ++ Path = Req:get(path),
  % error_logger:info_msg("path: ~p~n", [Path]),
  case Req:get(method) of
    'GET' ->
      case filelib:is_file(DocRoot ++ "/" ++ Path) of
        true -> 
          case filelib:is_file(DocRoot ++ "/" ++ Path ++ "/index.html") of
            true ->
              {ok, Data} = file:read_file(DocRoot ++ "/" ++ Path ++ "/index.html"),
              Req:ok({"text/html",Data});
            false ->
              {ok, Data} = file:read_file(DocRoot++"/"++Path),
              Req:ok({"text/html",Data})
          end;
        false ->
          case Path of
            "rpc/" ++ FName -> rpc_invoke(FName, Req);
            _ -> Req:not_found()
          end
      end;
    _ ->
      Req:not_found()
  end.
%%====================================================================
%% Internal functions
%%====================================================================

rpc_invoke(Path, Req) ->
  [Meth, Arg] = [list_to_atom(M) || M <- string:tokens(Path, "/")],
  Result = web_rpc:Meth(Arg),
  % error_logger:info_msg("invoking web_rpc:~p(~p) got ~p~n", [Meth, Arg, Result]),
  Req:ok({"application/json", mochijson:encode(Result)}).

get_option(Option, Options) ->
  {proplists:get_value(Option, Options), proplists:delete(Option, Options)}.
    
find_web_dir() ->
  case filelib:is_dir(WebDir1 = filename:join([filename:dirname(code:which(?MODULE)), "..", "web"])) of
    true -> WebDir1;
    false -> filename:join([filename:dirname(code:which(?MODULE)), "..", "priv", "web"])
  end.

