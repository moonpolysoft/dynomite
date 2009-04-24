-include_lib("eunit/include/eunit.hrl").

relative_path_test() ->
  Dir = "/blah/blaa/bloo/blee/1/",
  File = "/bleep/bloop/blop/blorp/1/file.idx",
  ?assertEqual("/blah/blaa/bloo/blee/1/file.idx", relative_path(Dir, File)).

simple_send_test_() ->
  {timeout, 120, ?_test(test_simple_send())}.
  
test_simple_send() ->
  process_flag(trap_exit, true),
  test_cleanup(),
  test_setup(),
  Ref = make_ref(),
  Receiver = spawn_link(fun() -> receive_bootstrap(priv_dir("b"), Ref) end),
  send_bootstrap(priv_dir("a"), Receiver, Ref),
  ?assertEqual(file:read_file(data_file("a")), file:read_file(data_file("b"))).
  
test_cleanup() ->
  rm_rf_dir(priv_dir("a")),
  rm_rf_dir(priv_dir("b")).
  
test_setup() ->
  ok = crypto:start(),
  {ok, IO} = file:open(data_file("a"), [raw, binary, write]),
  lists:foreach(fun(_) ->
      D = << << X:8 >> || X <- lists:seq(1, 1024) >>,
      file:write(IO, D)
    end, lists:seq(1, 100)),
  ok = file:close(IO).
  
priv_dir(Root) ->
  Dir = filename:join([t:config(priv_dir), "bootstrap", Root, "1"]),
  filelib:ensure_dir(filename:join([Dir, "bootstrap"])),
  Dir.
  
data_file(Root) ->
  filename:join([priv_dir(Root), "bootstrap"]).
  
rm_rf_dir(Dir) ->
  ?cmd("rm -rf " ++ Dir).