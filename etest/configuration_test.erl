-include_lib("eunit/include/eunit.hrl").

config_parsing_test() ->
  JSON = "{\"blocksize\": 4096,\"text_port\": 11222,\"thrift_port\": 9200,\"web_port\": 8080,\"directory\": \"/Users/cliff/data/tmp\",\"storage_mod\": \"dets_storage\",\"n\": 1,\"r\": 1,\"w\": 1,\"q\": 6}",
  Config = decode_json(mochijson:decode(JSON)),
  ?assertEqual(#config{
    blocksize=4096,
    text_port=11222,
    thrift_port=9200,
    web_port=8080,
    directory="/Users/cliff/data/tmp",
    storage_mod="dets_storage",
    n=1,
    r=1,
    w=1,
    q=6}, Config).
    
config_merge_test() ->
  Remote = #config{blocksize=20,storage_mod="dets_storage",n=3,r=3,w=3,q=4},
  Local = #config{
    blocksize=4096,
    text_port=11222,
    thrift_port=9200,
    web_port=8080,
    directory="/Users/cliff/data/tmp",
    storage_mod="derp_storage",
    n=1,
    r=1,
    w=1,
    q=6},
  Merged = merge_configs(Remote, Local),
  ?assertEqual(#config{
    blocksize=20,
    text_port=11222,
    thrift_port=9200,
    web_port=8080,
    directory="/Users/cliff/data/tmp",
    storage_mod="dets_storage",
    n=3,
    r=3,
    w=3,
    q=4}, Merged).