{application, dynomite,
  [{description, "Dynomite Storage Node"},
   {mod, {dynomite_app, []}},
   {vsn, "?VERSION"},
   {modules, 
   [
      block_server, bootstrap, commands, configuration, couch_btree, couch_file, dets_storage, dict_storage, dmerkle,
      dmtree, dynomite, dynomite_app, dynomite_prof, dynomite_sup, dynomite_thrift_service, dynomite_web, fail_storage, fnv, fs_storage,
      lib_misc, mediator, membership, mnesia_storage, murmur, partitions, rate, socket_server, stats_server, storage_manager,
      storage_server, storage_server_sup, stream, sync_manager, sync_server, sync_server_sup, tc_storage, ulimit, vector_clock,
      web_rpc
    ]},
    {registered, []},
    {applications, [kernel, stdlib, sasl, crypto, mochiweb, thrift]}
  ]}.