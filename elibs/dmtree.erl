%%%-------------------------------------------------------------------
%%% File:      dmtree.erl
%%% @author    Cliff Moon <cliff@powerset.com> []
%%% @copyright 2008 Cliff Moon
%%% @doc  
%%%
%%% @end  
%%%
%%% @since 2008-08-05 by Cliff Moon
%%%-------------------------------------------------------------------
-module(dmtree).
-author('cliff@powerset.com').

-behaviour(gen_server).

%% API
-export([start_link/2, stop/1, tx_begin/1, tx_commit/1, tx_rollback/1, d/1, root/1, block_size/1, update_root/2, write/2, delete/2, read/2, read_key/2, delete_key/3, write_key/3, filename/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("dmerkle.hrl").
-include("common.hrl").
-include_lib("kernel/include/file.hrl").

-record(dmtree, {file, size=0, virtsize=0, d, blocksize, filename, ops=[], freepointer=0, rootpointer=0}).

-ifdef(TEST).
-include("etest/dmtree_test.erl").
-endif.

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% @spec start_link() -> {ok,Pid} | ignore | {error,Error}
%% @doc Starts the server
%% @end 
%%--------------------------------------------------------------------
start_link(FileName, BlockSize) ->
  gen_server:start_link(dmtree, [FileName, BlockSize], []).
  
tx_begin(Pid) ->
  gen_server:call(Pid, tx_begin).
  
tx_commit(Pid) ->
  gen_server:call(Pid, tx_commit).
  
tx_rollback(Pid) ->
  gen_server:call(Pid, tx_rollback).
  
root(Pid) ->
  gen_server:call(Pid, root).
  
d(Pid) ->
  gen_server:call(Pid, d).
  
block_size(Pid) ->
  gen_server:call(Pid, block_size).
  
update_root(Node, Pid) ->
  gen_server:call(Pid, {update_root, Node}).
    
stop(Pid) ->
  gen_server:cast(Pid, close).
  
write(Node, Pid) ->
  gen_server:call(Pid, {write, Node}).
  
delete(Offset, Pid) ->
  gen_server:call(Pid, {delete, Offset}).
  
read(Offset, Pid) ->
  gen_server:call(Pid, {read, Offset}).
  
read_key(Offset, Pid) ->
  gen_server:call(Pid, {read_key, Offset}).
  
delete_key(Offset, Key, Pid) ->
  gen_server:call(Pid, {delete_key, Offset, Key}).
  
write_key(Offset, Key, Pid) ->
  gen_server:call(Pid, {write_key, Offset, Key}).
  
filename(Pid) ->
  gen_server:call(Pid, filename).

%%====================================================================
%% gen_server callbacks
%%====================================================================

%%--------------------------------------------------------------------
%% @spec init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% @doc Initiates the server
%% @end 
%%--------------------------------------------------------------------
init([FileName, BlockSize]) ->
  filelib:ensure_dir(FileName),
  {ok, File} = file:open(FileName, [read, write, binary]),
  {ok, FileInfo} = file:read_file_info(FileName),
  FileSize = FileInfo#file_info.size,
  case read_header(File) of
    {ok, Header} -> {ok, create_or_read_root(Header#dmtree{filename=FileName,file=File,size=FileSize})};
    {error, Msg} -> {stop, Msg};
    eof ->
      D = d_from_blocksize(BlockSize),
      AlignedBlockSize = blocksize_from_d(D),
      T = create_or_read_root(#dmtree{file=File,d=D,blocksize=AlignedBlockSize,filename=FileName,size=?HEADER_SIZE}),
      % ?infoFmt("created T ~p~n", [T]),
      flush(File, T#dmtree.ops),
      {ok, T#dmtree{ops=[],size=?HEADER_SIZE + AlignedBlockSize}}
  end.

%%--------------------------------------------------------------------
%% @spec 
%% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% @doc Handling call messages
%% @end 
%%--------------------------------------------------------------------
handle_call(tx_begin, _From, State = #dmtree{}) ->
  {reply, ok, State#dmtree{ops=[]}};
  
handle_call(tx_commit, _From, State = #dmtree{ops=Ops,file=File,size=Size,virtsize=VirtSize}) ->
  % ?infoFmt("commiting btree changes to disk size ~p virtsize ~p~n operations~p~n", [Size, VirtSize, lists:reverse(Ops)]),
  case flush(File, Ops) of
    ok -> {reply, ok, State#dmtree{ops=[], size=Size+VirtSize, virtsize=0}};
    {error, Reasons} -> {stop, Reasons, State}
  end;
  
handle_call(tx_rollback, _From, State = #dmtree{}) ->
  {reply, ok, State#dmtree{ops=[], virtsize=0}};
  
handle_call(d, _From, State = #dmtree{d=D}) ->
  {reply, D, State};
  
handle_call(root, _From, State = #dmtree{rootpointer=Ptr}) ->
  {reply, int_read(Ptr, State), State};
  
handle_call(block_size, _From, State = #dmtree{blocksize=BlockSize}) ->
  {reply, BlockSize, State};
  
handle_call({update_root, Node}, _From, State = #dmtree{}) ->
  {reply, self(), write_header(State#dmtree{rootpointer=offset(Node)})};
  
handle_call({write, Node}, _From, State = #dmtree{}) ->
  {N, T} = int_write(Node, State),
  {reply, N, T};
  
handle_call({delete, Offset}, _From, State = #dmtree{}) ->
  {reply, self(), int_delete(Offset, State)};
  
handle_call({read, Offset}, _From, State = #dmtree{}) ->
  {reply, int_read(Offset, State), State};
  
handle_call({read_key, Offset}, _From, State = #dmtree{}) ->
  {reply, int_read_key(Offset, State), State};
  
handle_call({delete_key, Offset, Key}, _From, State = #dmtree{}) ->
  {reply, self(), int_delete_key(Offset, Key, State)};
  
handle_call({write_key, Offset, Key}, _From, State = #dmtree{}) ->
  {Offset2, State2} = int_write_key(Offset, Key, State),
  {reply, Offset2, State2};
  
handle_call(filename, _From, State = #dmtree{filename=Filename}) ->
  {reply, Filename, State}.

%%--------------------------------------------------------------------
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% @doc Handling cast messages
%% @end 
%%--------------------------------------------------------------------
handle_cast(close, State) ->
    {stop, shutdown, State}.

%%--------------------------------------------------------------------
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% @doc Handling all non call/cast messages
%% @end 
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @spec terminate(Reason, State) -> void()
%% @doc This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%% @end 
%%--------------------------------------------------------------------
terminate(_Reason, #dmtree{file=File}) ->
  % error_logger:info_msg("shutting down and closing~n"),
  ok = file:close(File).

%%--------------------------------------------------------------------
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @doc Convert process state when code is changed
%% @end 
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions =================READ / WRITE operations
%%--------------------------------------------------------------------
write_header(Tree = #dmtree{file=File}) ->
  % ok = file:pwrite(File,0,serialize_header(Tree)),
  % Tree.
  {_, Tree2} = add_operation(0, serialize_header(Tree), Tree),
  Tree2.
  
read_header(File) ->
  case file:pread(File, 0, ?HEADER_SIZE) of
    {ok, Bin} -> deserialize_header(Bin);
    eof -> eof;
    {error, Msg} -> {error, Msg}
  end.
  
int_read(0, Tree) ->
  throw("tried to read a node from the null pointer");

int_read(Offset, #dmtree{file=File,blocksize=BlockSize,ops=Ops}) ->
  case lists:keysearch(Offset, 1, Ops) of
    {value, {Offset, Bin}} -> deserialize(Bin, Offset);
    false -> 
      case file:pread(File, Offset, BlockSize) of
        {ok, Bin} -> deserialize(Bin, Offset);
        eof -> 
          error_logger:info_msg("hit an eof for offset ~p", [Offset]),
          undefined;
        {error, Reason} -> 
          error_logger:info_msg("error ~p at offset", [Reason, Offset]),
          undefined
      end
  end.
  
int_write(Node, Tree = #dmtree{file=File,blocksize=BlockSize}) ->
  % ?infoFmt("int_write ~p~n", [Node]),
  Offset = offset(Node),
  Bin = serialize(Node, BlockSize),
  {Offset2, Tree2} = take_free_offset(Offset, Tree),
  {Offset3, Tree3} = add_operation(Offset2, Bin, Tree2),
  {offset(Node, Offset3), Tree3}.
  
int_delete(Offset, Tree = #dmtree{file=File,blocksize=BlockSize,freepointer=Pointer}) ->
  {_, Tree2} = int_write(#free{offset=Offset,pointer=Pointer}, Tree),
  write_header(Tree2#dmtree{freepointer=Offset}).
  
%this gotta change at some point
int_read_key(Offset, #dmtree{file=File}) ->
  file:position(File, Offset),
  int_read_key(File, []);
  
int_read_key(Keys, [0 | Key]) ->
  K = lists:reverse(Key),
  % ?infoFmt("int_read_key ~p~n", [K]),
  K;

int_read_key(Keys, Key) ->
  case file:read(Keys, 1) of
    {ok, <<Char:8>>} -> int_read_key(Keys, [Char|Key]);
    Other -> Other
  end.
  
% int_write_key(eof, Key, Tree = #dmtree{blocksize=BlockSize}) when length(Key) < BlockSize ->
%   
  
int_write_key(Offset, Key, Tree) ->
  add_operation(Offset, [Key,0], Tree).
  
int_delete_key(Offset, Key, Tree = #dmtree{}) ->
  
  Tree.
  
%%--------------------------------
%% ============SUPPORT FUNCTIONS
%%--------------------------------
flush(File, Ops) ->
  file:pwrite(File, lists:reverse(Ops)).

add_operation(eof, Bin, Tree = #dmtree{size=Size,virtsize=VirtSize,ops=Ops}) ->
  BinSize = true_size(Bin),
  Offset = Size+VirtSize,
  {Offset, Tree#dmtree{virtsize=VirtSize+BinSize, ops=[{Offset,Bin}|Ops]}};
  
add_operation(Offset, Bin, Tree = #dmtree{ops=Ops}) ->
  %we might wanna clobber rewrites
  case lists:keytake(Offset, 1, Ops) of
    {value, {Offset, _}, Ops2} -> {Offset, Tree#dmtree{ops=[{Offset,Bin}|Ops2]}};
    false -> {Offset, Tree#dmtree{ops=[{Offset,Bin}|Ops]}}
  end.
  
true_size(List) when is_list(List) ->
  lists:foldl(fun(E, Sum) -> true_size(E) + Sum end, 0, lists:flatten(List));
  
true_size(Bin) when is_binary(Bin) ->
  byte_size(Bin);
  
true_size(_) -> 1.

create_or_read_root(Tree = #dmtree{file=File,blocksize=BlockSize,rootpointer=0}) ->
  {Root, Tree2} = int_write(#leaf{offset=?HEADER_SIZE}, Tree),
  write_header(Tree2#dmtree{rootpointer=offset(Root)});
  
create_or_read_root(Tree = #dmtree{file=File,blocksize=BlockSize,rootpointer=Ptr}) ->
  Tree.

take_free_offset(eof, Tree = #dmtree{file=File,blocksize=BlockSize,freepointer=FreePtr}) when FreePtr > 0 ->
  Offset = FreePtr,
  #free{pointer=NewFreePointer} = int_read(FreePtr, Tree),
  {Offset, write_header(Tree#dmtree{freepointer=NewFreePointer})};

take_free_offset(Offset, Tree) ->
  {Offset, Tree}.

serialize_header(#dmtree{blocksize=BlockSize, freepointer=FreePtr, rootpointer=RootPtr}) ->
  FreeSpace = 64*8,
  <<?VERSION:8, BlockSize:32, FreePtr:64, RootPtr:64, 0:FreeSpace>>.

%this will try and match the current version, if it doesn't then we gotta punch out
deserialize_header(<<?VERSION:8, BlockSize:32, FreePtr:64, RootPtr:64, _Reserved:64/binary>>) ->
  {ok, #dmtree{blocksize=BlockSize,d=d_from_blocksize(BlockSize),freepointer=FreePtr,rootpointer=RootPtr}};

%hit the canopy
deserialize_header(BinHeader) ->
  case BinHeader of
    <<Version:8, _/binary>> -> {error, ?fmt("Mismatched version.  Cannot read version ~p", [Version])};
    _ -> {error, "Cannot read version.  Dmerkle is corrupted."}
  end.

%node is denoted by a 0
deserialize(<<0:8, Binary/binary>>, Offset) ->
  D = d_from_blocksize(byte_size(Binary) + 1),
  KeyBinSize = D*4,
  ChildBinSize = (D+1)*12,
  <<M:32, KeyBin:KeyBinSize/binary, ChildBin:ChildBinSize/binary>> = Binary,
  if
    M > D -> error_logger:info_msg("M is larger than D M ~p D ~p offset~p~n", [M, D, Offset]);
    true -> ok
  end,
  Keys = unpack_keys(M, KeyBin),
  Children = unpack_children(M+1, ChildBin),
  #node{m=M,children=Children,keys=Keys,offset=Offset};

deserialize(<<1:8, Bin/binary>>, Offset) ->
  D = d_from_blocksize(byte_size(Bin) + 1),
  ValuesBinSize = D*16,
  <<M:32, ValuesBin:ValuesBinSize/binary, _/binary>> = Bin,
  Values = unpack_values(M, ValuesBin),
  #leaf{m=M,values=Values,offset=Offset};

deserialize(<<3:8, Pointer:64, _/binary>>, Offset) ->
  #free{offset=Offset,pointer=Pointer}.

serialize(Free = #free{pointer=Pointer}, BlockSize) ->
  LeftOverBits = (BlockSize - 9) * 8,
  <<3:8,Pointer:64,0:LeftOverBits>>;

serialize(Node = #node{keys=Keys,children=Children,m=M}, BlockSize) ->
  D = d_from_blocksize(BlockSize),
  if
    M > D -> error_logger:info_msg("M is larger than D M ~p D ~p~n", [M, D]);
    length(Keys) == length(Children) -> error_logger:info_msg("There are as many children as keys for ~p~n", [Node]);
    true -> ok
  end,
  KeyBin = pack_keys(Keys, D),
  ChildBin = pack_children(Children, D+1),
  LeftOverBits = (BlockSize - byte_size(KeyBin) - byte_size(ChildBin) - 5)*8,
  OutBin = <<0:8, M:32, KeyBin/binary, ChildBin/binary, 0:LeftOverBits>>,
  if 
    byte_size(OutBin) /= BlockSize ->
      error_logger:info_msg("outbin is wrong size! keys: ~p children: ~p m: ~p outbin ~p~n", [length(Keys), length(Children), M, byte_size(OutBin)]);
    true -> ok
  end,
  OutBin;

serialize(#leaf{values=Values,m=M}, BlockSize) ->
  D = d_from_blocksize(BlockSize),
  if
    M > D -> error_logger:info_msg("M is larger than D M ~p D ~p~n", [M, D]);
    true -> ok
  end,
  ValuesBin = pack_values(Values),
  LeftOverBits = (BlockSize - byte_size(ValuesBin) - 5)*8,
  OutBin = <<1:8, M:32, ValuesBin/binary, 0:LeftOverBits>>,
  if 
    byte_size(OutBin) /= BlockSize ->
      error_logger:info_msg("outbin is wrong size! values: ~p m: ~p outbin ~p~n", [length(Values), M, byte_size(OutBin)]);
    true -> ok
  end,
  OutBin.

pack_values(Values) ->
  << <<KeyHash:32, KeyPointer:64, ValHash:32>> || {KeyHash,KeyPointer,ValHash} <- Values >>.

pack_keys(Keys, D) ->
  Bin = << <<KeyHash:32>> || KeyHash <- Keys >>,
  BinSize = byte_size(Bin),
  LeftOverBits = D*4*8 - BinSize*8,
  <<Bin/binary, 0:LeftOverBits>>.

pack_children(Children, D) ->
  Bin = << <<ChildHash:32, ChildPtr:64>> || {ChildHash,ChildPtr} <- Children >>,
  BinSize = byte_size(Bin),
  LeftOverBits = D*12*8 - BinSize*8,
  <<Bin/binary, 0:LeftOverBits>>.

unpack_keys(M, Bin) ->
  [ KeyHash || <<KeyHash:32>> <= truncate(M*4, Bin) ].

unpack_children(M, Bin) ->
  [{ChildHash,ChildPtr} || <<ChildHash:32, ChildPtr:64>> <= truncate(M*12, Bin)].

unpack_values(M, Bin) ->
  [{KeyHash,KeyPointer,ValueHash} || <<KeyHash:32,KeyPointer:64,ValueHash:32>> <= truncate(M*16, Bin)].
  
truncate(Size, Bin) ->
  <<TruncBin:Size/binary, _/binary>> = Bin,
  TruncBin.
  
deserialize(Bin) when byte_size(Bin) < 8 ->
  {byte_size(Bin), eos};
  
deserialize(<<NextPtr:64, Rest/binary>>) ->
  {byte_size(Rest) + 8, NextPtr}.
  
d_from_blocksize(BlockSize) ->
  trunc((BlockSize - 17)/16).

blocksize_from_d(D) ->
  trunc(16*D + 17).
  
offset(#leaf{offset=Offset}) -> Offset;
offset(#free{offset=Offset}) -> Offset;
offset(#node{offset=Offset}) -> Offset.

offset(Leaf = #leaf{}, Offset) -> Leaf#leaf{offset=Offset};
offset(Free = #free{}, Offset) -> Free#free{offset=Offset};
offset(Node = #node{}, Offset) -> Node#node{offset=Offset}.
