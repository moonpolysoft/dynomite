
-ifndef(CONFIG_HRL).
-define(CONFIG_HRL, true).

-record(config, {n=3, r=1, w=1, q=6, directory, web_port, text_port=11222, storage_mod=dets_storage, blocksize=4096, thrift_port=9200, pb_port=9300}).

-endif.
