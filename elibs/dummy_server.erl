-module(dummy_server).

-export([start_link/1, stop/1]).

-behavior(gen_server).
%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

start_link(Name) ->
  gen_server:start_link({local, Name}, ?MODULE, [], []).
  
stop(Server) ->
  gen_server:cast(Server, stop).
         
%%%%%% DUMMY GEN_SERVER %%%%%%%%%%%%%%%%%%%

init([]) ->
  {ok, undefined}.

handle_call(connections, _From, State) ->
  {reply, 0, State}.

handle_cast(stop, State) ->
  {stop, shutdown, State};

handle_cast(_Req, State) ->
  {noreply, State}.

handle_info(_Msg, State) ->
  {noreply, State}.

terminate(_Reason, State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.