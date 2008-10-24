-include_lib("eunit.hrl").

-define(NODES, [one, two, three, four, five, six, seven, eight]).
-define(PARTITIONS, [{one, 1}, {two, 2}, {three, 3}, {four, 4}, {five, 5}, {six, 6}, {seven, 7}, {eight, 8}]).

% partition_range_test() ->
%   (2 bsl 21) = partition_range(10),
%   (2 bsl 14) = partition_range(17).
% 
% create_partitions_length_test() ->
%   State = create_initial_state(#config{q=10}),
%   Partitions = length(State#membership.partitions),
%   Partitions = (2 bsl 9).
  
  %steal_partitions(ForNode, Partitions, Nodes, #config{q=Q})
% third_node_join_test() ->
%   Partitions = lists:flatten(
%     lists:map(fun(Node) -> lists:map(fun(N) -> {Node, N} end, lists:seq(1,32)) end, [a@blah, b@blah])
%     ),
%   NewPartitions = steal_partitions(c@blah, Partitions, [c@blah, a@blah, b@blah], #config{q=6}),
%   error_logger:info_msg("partitions: ~p~n", [NewPartitions]),
%   {NewPart, _} = lists:partition(fun({E,_}) -> c@blah == E end, NewPartitions),
%   error_logger:info_msg("newpart: ~p~n", [NewPart]),
%   timer:sleep(100),
%   21 = length(NewPart),
%   64 = length(NewPartitions).
%   
% fourth_node_join_test() ->
%   Partitions = lists:flatten(
%     lists:map(fun(Node) -> lists:map(fun(N) -> {Node, N} end, lists:seq(1,32)) end, [a@blah, b@blah])
%     ),
%   NewPartitions = steal_partitions(c@blah, Partitions, [c@blah, a@blah, b@blah], #config{q=6}),
%   NewPartitions1 = steal_partitions(d@blah, NewPartitions, [d@blah, a@blah, b@blah, c@blah], #config{q=6}),
%   {DPart, _} = lists:partition(fun({E,_}) -> d@blah == E end, NewPartitions1),
%   {CPart, _} = lists:partition(fun({E,_}) -> c@blah == E end, NewPartitions1),
%   {BPart, _} = lists:partition(fun({E,_}) -> b@blah == E end, NewPartitions1),
%   {APart, _} = lists:partition(fun({E,_}) -> a@blah == E end, NewPartitions1),
%   timer:sleep(100),
%   16 = length(DPart),
%   16 = length(CPart),
%   16 = length(BPart),
%   16 = length(APart).
  
% within_true_test() ->
%   {true, one} = within(3, one, three, ?NODES),
%   {true, six} = within(3, six, eight, ?NODES),
%   {true, six} = within(3, six, seven, ?NODES).
%   
% within_false_test() ->
%   false = within(2, one, three, ?NODES),
%   false = within(3, one, eight, ?NODES).
%   
% within_swapped_test() ->
%   {true, one} = within(3, three, one, ?NODES),
%   {true, one} = within(3, two, one, ?NODES),
%   {true, seven} = within(2, eight, seven, ?NODES).
%   
% join_node_split_test() ->
%   {ok, Pid} = membership:start_link(#config{q=10,n=1,r=1,w=1}),
%   membership:join_node(node(), two),
%   P1 = membership:partitions_for_node(two, master),
%   512 = length(P1),
%   membership:stop(),
%   receive _ -> ok end.
%   
% find_partition_simple_test() ->
%   1 = find_partition(500, 10).
%   
% find_partition_middle_test() ->
%   71303169 = find_partition(71303169+800, 10).
%   
% find_partition_beginning_test() ->
%   1 = find_partition(1, 10).
%   
% find_partition_end_test() ->
%   3590324225 = find_partition(3594518528, 10).
%   
% find_partition_very_end_test() ->
%   4290772993 = find_partition(4294967296, 10).
%   
% index_for_partition_test() ->
%   1 = index_for_partition(1, 10).
%   
% index_for_partition_end_test() ->
%   1024 = index_for_partition(4290772993, 10).
%   
% n_nodes_whole_test() ->
%   [a, b, c] = n_nodes(a, 3, [a, b, c]).
%   
% n_nodes_simple_test() ->
%   [a] = n_nodes(a, 1, [a, b, c]).
%   
% n_nodes_wrap_test() ->
%   [d, e, a] = n_nodes(d, 3, [a, b, c, d, e]).
%   
% n_nodes_sublist_test() ->
%   [b, c] = n_nodes(b, 2, [a, b, c, d, e]).
%   
% merge_partitions_simple_test() ->
%   PartA = [{a,1},{b,2},{c,3}],
%   PartB = [{a,1},{b,2},{c,3}],
%   Nodes = [a, b, c],
%   [{a,1},{b,2},{c,3}] = merge_partitions(PartA, PartB, [], 1, Nodes).
%   
% merge_partitions_overlap_test() ->
%   PartA = [{a,1},{b,2},{c,3}],
%   PartB = [{c,1}],
%   Nodes = [a, b, c],
%   [{a,1},{b,2},{c,3}] = merge_partitions(PartA, PartB, [], 3, Nodes).
%   
% %%%%% test out the api
% 
% partitions_for_node_master_simple_test() ->
%   Config = #config{n=1,r=1,w=1,q=10},
%   {ok, Pid} = membership:start_link(Config),
%   Partitions = membership:partitions_for_node(node(), master),
%   1024 = length(Partitions),
%   membership:stop(),
%   receive _ -> ok end.
%   
% partitions_for_node_all_simple_test() ->
%   Config = #config{n=1,r=1,w=1,q=10},
%   {ok, Pid} = membership:start_link(Config),
%   Partitions = membership:partitions_for_node(node(), all),
%   1024 = length(Partitions),
%   membership:stop(),
%   receive _ -> ok end.
%   
% partitions_for_node_master_two_nodes_test() ->
%   Config = #config{n=1,r=1,w=1,q=10},
%   {ok, Pid} = membership:start_link(Config),
%   membership:join_node(node(), 'dynomite@blah.blah'),
%   Partitions = membership:partitions_for_node('dynomite@blah.blah', master),
%   timer:sleep(100),
%   512 = length(Partitions),
%   membership:stop(),
%   receive _ -> ok end.
%   
% nodes_for_key_test() ->
%   Config = #config{n=1,r=1,w=1,q=6},
%   {ok, Pid} = membership:start_link(Config),
%   Node = node(),
%   [Node] = membership:nodes_for_key(<<"original-natural-Melbourne_international_exhibition_1880.jpg">>),
%   membership:stop(),
%   receive _ -> ok end.
%   
% join_node_weirdness_test() ->
%   Config = #config{n=1,r=1,w=1,q=6},
%   {ok, Pid} = membership:start_link(Config),
%   lists:foreach(fun(N) ->
%       membership:join_node(node(), list_to_atom(lists:concat([N, '@blah.blah'])))
%     end, lists:seq(1, 10)),
%   membership:stop(),
%   receive _ -> ok end.
%   
% reload_storage_servers_startup_test() ->
%   Config = #config{n=1,r=1,w=1,q=6,storage_mod=dict_storage,live=true},
%   {ok, SupPid} = storage_server_sup:start_link(Config),
%   {ok, MemPid} = membership:start_link(Config),
%   64 = length(supervisor:which_children(storage_server_sup)),
%   membership:stop(),
%   receive _ -> ok end,
%   exit(SupPid, shutdown),
%   receive _ -> ok end.
%   
% reload_storage_servers_rebalance_test() ->
%   Config = #config{n=1,r=1,w=1,q=6,storage_mod=dict_storage,live=true},
%   {ok, SupPid} = storage_server_sup:start_link(Config),
%   {ok, MemPid} = membership:start_link(Config),
%   membership:join_node(node(), 'newnode'),
%   32 = length(supervisor:which_children(storage_server_sup)),
%   membership:stop(),
%   receive _ -> ok end,
%   exit(SupPid, shutdown),
%   receive _ -> ok end.
  
