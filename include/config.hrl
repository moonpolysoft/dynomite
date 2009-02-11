
-ifndef(CONFIG_HRL).
-define(CONFIG_HRL, true).

-record(config, {n, r, w, q, directory, port, storage_mod, blocksize=4096, thrift_port=9200}).

-endif.
