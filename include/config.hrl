
-ifndef(CONFIG_HRL).
-define(CONFIG_HRL, true).

-record(config, {n, r, w, q, directory, port, storage_mod, live=false}).

-endif.