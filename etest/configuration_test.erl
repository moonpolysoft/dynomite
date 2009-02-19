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