priv_dir() ->
  Dir = filename:join([t:config(priv_dir), "data", atom_to_list(?MODULE)]),
  filelib:ensure_dir(filename:join(Dir, atom_to_list(?MODULE))),
  Dir.