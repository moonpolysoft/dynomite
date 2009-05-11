%%%-------------------------------------------------------------------
%%% File:      stub.erl
%%% @author    Cliff Moon <> []
%%% @copyright 2009 Cliff Moon
%%% @doc  
%%%
%%% @end  
%%%
%%% @since 2009-05-10 by Cliff Moon
%%%-------------------------------------------------------------------
-module(stub).
-author('cliff@powerset.com').

-behaviour(gen_server).

%% API
-export([stub/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("../include/common.hrl").

-record(state, {}).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% @spec start_link() -> {ok,Pid} | ignore | {error,Error}
%% @doc Starts the server
%% @end 
%%--------------------------------------------------------------------
stub(Module, Function, Fun, Times) when is_function(Fun) ->
  gen_server:start_link({local, generate_name(Module, Function, Fun)}, ?MODULE, [Module, Function, Fun, Times], []).

%%====================================================================
%% gen_server callbacks
%%====================================================================

%%--------------------------------------------------------------------
%% @spec init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% @doc Initiates the server
%% @end 
%%--------------------------------------------------------------------
init([Module, Function, Fun, Times]) ->

  {ok, #state{}}.

%%--------------------------------------------------------------------
%% @spec 
%% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% @doc Handling call messages
%% @end 
%%--------------------------------------------------------------------
handle_call(_Request, _From, State) ->
  Reply = ok,
  {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% @doc Handling cast messages
%% @end 
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% @doc Handling all non call/cast messages
%% @end 
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------
%% @spec terminate(Reason, State) -> void()
%% @doc This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%% @end 
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
  ok.

%%--------------------------------------------------------------------
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @doc Convert process state when code is changed
%% @end 
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------
generate_name(Module, Function, _Fun) ->
  list_to_atom(lists:concat([Module, Function])).
  
stub_function(Module, Function, Arity, Ret) when is_function(Ret) ->
  {_, Bin, _} = code:get_object_code(Module),
  {ok, {Module,[{abstract_code,{raw_abstract_v1,Forms}}]}} = beam_lib:chunks(Bin, [abstract_code]),
  StubbedForms = replace_function(Function, Arity, Ret, Forms),
  
  
  
  {Prefix, Functions} = lists:splitwith(fun(Form) -> element(1, Form) =/= function end, Forms),
  Function = {function,1,Name,Arity,[{clause,1,generate_variables(Arity), [], generate_expression(mock, stub_proxy_call, Module, Name, Arity)}]},
  RegName = list_to_atom(lists:concat([Module, "_", Name, "_stub"])),
  Pid = spawn_link(fun() ->
      stub_function_loop(Ret)
    end),
  register(RegName, Pid),
  Forms1 = Prefix ++ replace_function(Function, Functions),
  code:purge(Module),
  code:delete(Module),
  case compile:forms(Forms1, [binary]) of
    {ok, Module, Binary} -> code:load_binary(Module, atom_to_list(Module) ++ ".erl", Binary);
    Other -> Other
  end.
  
arity(Fun) when is_function(Fun) ->
  Props = erlang:fun_info(Fun),
  proplists:get_value(arity, Props).

replace_function(Module, Function, Arity, Ret, Forms) ->
  replace_function(Function, Arity, Ret, Forms, []).
  
replace_function(Module, Function, Arity, Ret, [], Acc) ->
  lists:reverse(Acc);
replace_function(Module, Function, Arity, Ret, [{function, Line, Function, Arity, _Clauses}|Forms], Acc) ->
  
