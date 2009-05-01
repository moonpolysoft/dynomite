%%%-------------------------------------------------------------------
%%% File:      commands.erl
%%% @author    Cliff Moon <cliff@powerset.com> []
%%% @copyright 2008 Cliff Moon
%%% @doc
%%%
%%% @end
%%%
%%% @since 2008-07-27 by Cliff Moon
%%%-------------------------------------------------------------------
-module(commands).
-author('cliff@powerset.com').

%% API
-export([start/0]).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% @spec
%% @doc
%% @end
%%--------------------------------------------------------------------

start() ->
  {Node, Module, Function, Arguments} = process_arguments([node, m, f, a]),
  Result = rpc:call(Node, Module, Function, Arguments),
  io:format("~p~n", [Result]),
  timer:sleep(10).

process_arguments(Args) ->
  process_arguments([], Args).

process_arguments(Res, []) ->
  list_to_tuple(lists:reverse(Res));

process_arguments(Res, [Arg|Args]) ->
  case init:get_argument(Arg) of
    {ok, Lists} ->
      ArgList = lists:flatten(lists:map(fun atomize/1, Lists)),
      if
        length(ArgList) == 1 ->
          [A] = ArgList,
          process_arguments([A|Res], Args);
        true ->
          process_arguments([ArgList|Res], Args)
      end;
    error ->
      process_arguments([[]|Res], Args)
  end.

atomize([E|L]) when is_list(E) ->
  lists:map(fun atomize/1, [E|L]);

atomize(L) -> list_to_atom(L).
