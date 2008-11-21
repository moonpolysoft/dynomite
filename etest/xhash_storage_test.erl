-include_lib("eunit.hrl").

initialize_data_test() ->
  setup(),
  {ok, XHash} = open("/Users/cliff/data/xhash", "xhash"),
  close(XHash),
  {ok, <<"XD", 0:16, 0:32, 0:64, 0:256>>} = file:read_file("/Users/cliff/data/xhash/xhash.xd").
  
initialize_index_test() ->
  setup(),
  {ok, XHash} = open("/Users/cliff/data/xhash", "xhash"),
  close(XHash),
  {ok, <<"XI", 0:16, 1024:32, 0:256, 0:65536>>} = file:read_file("/Users/cliff/data/xhash/xhash.xi").
  
initialize_read_test() ->
  setup(),
  {ok, XHash} = open("/Users/cliff/data/xhash", "xhash"),
  close(XHash),
  {ok, XHash1} = open("/Users/cliff/data/xhash", "xhash"),
  1024 = XHash1#xhash.capacity,
  0 = XHash1#xhash.size,
  0 = XHash1#xhash.head.
  
parent_test() ->
  0 = ?PARENT(1),
  1 = ?PARENT(3),
  11 = ?PARENT(75).
  
% insert_one_test() ->
%   setup(),
%   {ok, XHash} = open("/Users/cliff/data/xhash", "xhash"),
%   {ok, XHash1} = put("mahkey", "mahcontext", ["mahbvalues"], XHash),
%   1 = XHash1#xhash.size,
%   {ok, {"mahcontext", ["mahbvalues"]}} = get("mahkey", XHash1),
%   close(XHash1).
  
insert_20_test_() ->
  {timeout, 120, [{?LINE, fun() ->
    setup(),
    {ok, XHash} = open("/Users/cliff/data/xhash", "xhash"),
    XHash1 = lists:foldl(fun(E, X) ->
        {ok, XH} = put(lists:concat(["key", E]), context, [lists:concat(["value", E])], X),
        XH
      end, XHash, lists:seq(1,20)),
    % lists:foreach(fun(E) ->
    %     ExpKey = lists:concat(["key", E]),
    %     ExpVal = [lists:concat(["value", E])],
    %     {ok, {context, ExpVal}} = get(lists:concat(["key", E]), XHash1)
    %   end, lists:seq(1, 20)),
    % error_logger:info_msg("balance_dump: ~p", [dump(XHash1)]),
    timer:sleep(100)
  end}]}.
 %    
 % count_20_with_fold_test() ->
 %   setup(),
 %   {ok, XHash} = open("/Users/cliff/data/xhash", "xhash"),
 %   XHash1 = lists:foldl(fun(E, X) ->
 %       {ok, XH} = put(lists:concat(["key", E]), context, [E], X),
 %       XH
 %     end, XHash, lists:seq(1,20)),
 %   210 = fold(fun({Key, Ctx, [Value]}, Acc) ->
 %       Acc + Value
 %     end, XHash1, 0).
 %     
 % insert_1024_test_() ->
 %   {timeout, 400, [{?LINE, fun() ->
 %     setup(),
 %     {ok, XHash} = open("/Users/cliff/data/xhash", "xhash"),
 %     XHash1 = lists:foldl(fun(E, X) ->
 %         {ok, XH} = put(lists:concat(["key", E]), context, [lists:concat(["value", E])], X),
 %         XH
 %       end, XHash, lists:seq(1,1024)),
 %     % lists:foreach(fun(E) ->
 %     %     ExpKey = lists:concat(["key", E]),
 %     %     ExpVal = [lists:concat(["value", E])],
 %     %     {ok, {context, ExpVal}} = get(lists:concat(["key", E]), XHash1)
 %     %   end, lists:seq(1, 1024)).
 %     error_logger:info_msg("balance_dump: ~p", [dump(XHash1)]),
 %     timer:sleep(100)
 %   end}]}.
  
setup() ->
  filelib:ensure_dir("/Users/cliff/data/xhash"),
  file:delete("/Users/cliff/data/xhash/xhash.xd"),
  file:delete("/Users/cliff/data/xhash/xhash.xi").