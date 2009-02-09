%%%-------------------------------------------------------------------
%%% File:      bootstrap.erl
%%% @author    Cliff Moon <> []
%%% @copyright 2009 Cliff Moon
%%% @doc  
%%%
%%% @end  
%%%
%%% @since 2009-02-02 by Cliff Moon
%%%-------------------------------------------------------------------
-module(bootstrap).
-author('cliff@powerset.com').

-define(CHUNK, 4096).

%% API
-export([start/3]).

-include("common.hrl").

-ifdef(TEST).
-include("etest/bootstrap_test.erl").
-endif.

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% @spec 
%% @doc
%% @end 
%%--------------------------------------------------------------------

start(Directory, OldNode, Callback) when is_function(Callback) ->
  ?infoFmt("bootstrapping for directory ~p~n", [Directory]),
  Ref = make_ref(),
  Receiver = spawn_link(fun() -> 
      receive_bootstrap(Directory, Ref),
      % start the node once we receive the data
      Callback()
    end),
  spawn_link(OldNode, fun() -> send_bootstrap(Directory, Receiver, Ref) end).

%%====================================================================
%% Internal functions
%%====================================================================

receive_bootstrap(Directory, Ref) ->
  receive
    {Ref, filename, Filename} ->
      File = relative_path(Directory, Filename),
      filelib:ensure_dir(File),
      receive_file(File, Ref),
      receive_bootstrap(Directory, Ref);
    {Ref, done} -> ok
  end.
  
receive_file(File, Ref) ->
  {ok, IO} = file:open(File, [raw, binary, write]),
  Ctx = crypto:md5_init(),
  receive_contents(Ref, IO, Ctx).
  
receive_contents(Ref, IO, Ctx) ->
  receive
    {Ref, data, Data} ->
      file:write(IO, Data),
      receive_contents(Ref, IO, crypto:md5_update(Ctx, Data));
    {Ref, md5, Hash, Pid} ->
      case crypto:md5_final(Ctx) of
        Hash -> Pid ! {Ref, ok};
        _ -> Pid ! {Ref, error}
      end;
    {Ref, error, Reason} ->
      error_logger:info_msg("Bootstrap receive failed with reason ~p~n", [Reason]),
      exit(Reason);
    _ -> ok
  end.
  
send_bootstrap(Directory, Pid, Ref) ->
  filelib:fold_files(Directory, ".*", true, fun(File, _) ->
      send_file(File, Pid, Ref)
    end, nil),
  Pid ! {Ref, done}.
  
send_file(File, Pid, Ref) ->
  Pid ! {Ref, filename, File},
  {ok, IO} = file:open(File, [raw, binary, read]),
  Ctx = crypto:md5_init(),
  send_contents(Ref, Pid, IO, Ctx),
  ok = file:close(IO),
  % receive_ok(File, Pid, Ref).
  receive
    {Ref, ok} -> ok;
    {Ref, error} -> send_file(File, Pid, Ref)
  end.
  
send_contents(Ref, Pid, IO, Ctx) ->
  case file:read(IO, ?CHUNK) of
    {ok, Data} -> 
      Pid ! {Ref, data, Data},
      send_contents(Ref, Pid, IO, crypto:md5_update(Ctx, Data));
    eof ->
      Pid ! {Ref, md5, crypto:md5_final(Ctx), self()};
    {error, Reason} -> 
      error_logger:info_msg("Bootstrap send failed with reason ~p~n", [Reason]),
      Pid ! {Ref, error, Reason},
      exit(Reason)
  end.
  
relative_path(Directory, File) ->
  DirTokens = string:tokens(Directory, "/"),
  Dirname = lists:last(DirTokens),
  FileTokens = lists:takewhile(fun(T) -> 
      T =/= Dirname
    end, lists:reverse(string:tokens(File, "/"))),
  Final = string:join(DirTokens ++ lists:reverse(FileTokens), "/"),
  case Directory of
    [$/ | _] -> "/" ++ Final;
    _ -> Final
  end.
  