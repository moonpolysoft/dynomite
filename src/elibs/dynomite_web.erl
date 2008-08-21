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
-export([start/1, stop/0, loop/2]).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% @spec 
%% @doc
%% @end 
%%--------------------------------------------------------------------

start(Options) ->
    {DocRoot, Options1} = get_option(docroot, Options),
    Loop = fun (Req) ->
		   ?MODULE:loop(Req, DocRoot)
	   end,
    mochiweb_http:start([{name, ?MODULE}, {loop, Loop} | Options1]).

stop() ->
    mochiweb_http:stop(?MODULE).

loop(Req, DocRoot) ->
  "/" ++ Path = Req:get(path),
  error_logger:info_msg("path: ~p~n", [Path]),
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
  error_logger:info_msg("invoking web_rpc:~p(~p) got ~p~n", [Meth, Arg, Result]),
  Req:ok({rfc4627:mime_type(), rfc4627:encode(Result)}).

get_option(Option, Options) ->
    {proplists:get_value(Option, Options), proplists:delete(Option, Options)}.