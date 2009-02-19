-module(dynomite_thrift_service).

-export([start_link/0, stop/1,
         handle_function/2,

         % Internal
         put/3,
         get/1,
         has/1,
         remove/1
]).

-behavior(gen_server).
%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("config.hrl").
-include("common.hrl").
-include("dynomite_types.hrl").

%%%%% EXTERNAL INTERFACE %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link() ->
  Config = configuration:get_config(),
  case Config#config.thrift_port of
    undefined -> gen_server:start_link({local, dynomite_thrift}, ?MODULE, [], []);
    Port -> thrift_socket_server:start([
      {port, Port},
      {name, dynomite_thrift},
      {service, dynomite_thrift},
      {handler, ?MODULE},
      {max, 100},
      {socket_opts, [{recv_timeout, infinity}]}])
  end.

stop(Server) ->
  thrift_socket_server:stop(Server),
  ok.

%%%%%% DUMMY GEN_SERVER %%%%%%%%%%%%%%%%%%%

init([]) ->
  {ok, undefined}.
  
handle_call(connections, _From, State) ->
  {reply, 0, State}.
  
handle_cast(_Req, State) ->
  {noreply, State}.
  
handle_info(_Msg, State) ->
  {noreply, State}.
  
terminate(_Reason, State) ->
  ok.
  
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%%% THRIFT INTERFACE %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

handle_function(Function, Args) when is_atom(Function), is_tuple(Args) ->
    % ?infoFmt("handling thrift stuff in PID ~p~n", [self()]),
    case apply(?MODULE, Function, tuple_to_list(Args)) of
        ok -> ok;
        Reply -> {reply, Reply}
    end.



put(Key, ContextData, Data) when
  is_binary(Key),
  (ContextData =:= undefined orelse is_binary(ContextData)),
  is_binary(Data) ->
    Context = if
                  ContextData =:= undefined -> undefined;
                  erlang:byte_size(ContextData) > 0 -> binary_to_term(ContextData);
                  true -> undefined
              end,
    case mediator:put(binary_to_list(Key), {self(), Context}, Data) of
        {ok, N} -> N;
        {failure, Reason} -> throw(#failureException{message = iolist_to_binary(Reason)})
    end.
                   

get(Key) when is_binary(Key) ->
    case mediator:get(binary_to_list(Key)) of
        {ok, not_found} -> #getResult{results = []};
        {ok, {Context, Values}} ->
            #getResult{context = term_to_binary(Context),
                       results = Values};
        {failure, Error} ->
            throw(#failureException{message = iolist_to_binary(Error)})
    end.

has(Key) when is_binary(Key) ->
    case mediator:has_key(binary_to_list(Key)) of
        {ok, {Bool, N}} when is_boolean(Bool) ->
             N
    end.

remove(Key) when is_binary(Key) ->
    {ok, N} = mediator:delete(binary_to_list(Key)),
    N.

