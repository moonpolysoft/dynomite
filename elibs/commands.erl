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
  {Node, M, F, A} = case init:get_plain_arguments() of
    [] -> io:format("Sorry, I don't know what you want.");
    Args -> parse_args(Args)
  end,
  Results = rpc:call(Node, M, F, A),
  io:format("results: ~p~n", [Results]),
  timer:sleep(1000),
  halt().

%%====================================================================
%% Internal functions
%%====================================================================

parse_args(Args) -> parse_args(Args, {}).

parse_args([], {Node, M, F}) -> {Node, M, F, []};

parse_args([], Tuple) -> Tuple;

parse_args([Arg|Args], {Node, M, F, A}) ->
  parse_args(Args, {Node, M, F, [list_to_atom(Arg)|A]});

parse_args([Arg|Args], Tuple) ->
  parse_args(Args, erlang:append_element(Tuple, list_to_atom(Arg))).