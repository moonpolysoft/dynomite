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
-export([start_link/2, stop/1, read_block/3, write_block/3, read_key/2, write_key/3, index_name/1, key_name/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(dmtree, {file, blocksize, filename, freepointer=0, rootpointer=0, fp1=0, fp2=0, fp3=0, fp4=0, fp5=0}).

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
    
stop(Pid) ->
  gen_server:cast(Pid, close).
  
read_block(Pid, Offset, Size) ->
  gen_server:call(Pid, {read_block, Offset, Size}).
  
write_block(Pid, Offset, Data) ->
  gen_server:call(Pid, {write_block, Offset, Data}).
  
read_key(Pid, Offset) ->
  gen_server:call(Pid, {read_key, Offset}).
  
read_free(Pid, Offset) ->
  gen_server:call(Pid, {read_free, Offset}).
  
write_key(Pid, Offset, Key) ->
  gen_server:call(Pid, {write_key, Offset, Key}).

index_name(Path) ->
  lists:concat([Path, ".idx"]).

key_name(Path) ->
  lists:concat([Path, ".keys"]).

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
  {ok, File} = file:open(index_name(FileName), [read, write, binary]),
  Tree = case read_header(File) of
    {ok, Header} -> Header;
    eof ->
      D = d_from_blocksize(BlockSize),
      AlignedBlockSize = blocksize_from_d(D),
      T = #dmtree{file=File}
  {ok, #dmtree{file=File,d=D,blocksize=BlockSize}}
  
  Tree = case block_server:read_block(File, 0, ?HEADER_SIZE) of
    {ok, BinHeader} -> deserialize_header(BinHeader, #dmerkle{file=File,filename=FileName});
    eof ->
      D = d_from_blocksize(BlockSize),
      ModBlockSize = blocksize_from_d(D),
      T = #dmerkle{file=File,d=D,block=ModBlockSize,freepointer=0,rootpointer=0,filename=FileName},
      block_server:write_block(File, 0, serialize_header(T)),
      T
  end,
  {ok, Index} = file:open(index_name(FileName), [read, write, binary]),
  {ok, Keys} = file:open(key_name(FileName), [read, write, {read_ahead, BlockSize}]),
  {ok, #state{index=Index,keys=Keys}}.

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
handle_call({read_block, Offset, Size}, _From, State = #state{index=Index}) ->
  {reply, file:pread(Index, Offset, Size), State};
  
handle_call({write_block, Offset, Data}, _From, State = #state{index=Index}) ->
  {ok, Position} = file:position(Index, Offset),
  % error_logger:info_msg("writing ~p bytes at ~p~n", [byte_size(Data), Position]),
  Reply = file:write(Index, Data),
  {reply, {Reply, Position}, State};
  
handle_call({read_key, Offset}, _From, State = #state{keys=Keys}) ->
  file:position(Keys, Offset),
  {reply, int_read_key(Keys, []), State};
  
handle_call({read_free, Offset}, _From, State = #state{keys=Keys}) ->
  file:position(Keys, Offset),
  {reply, int_read_free(Keys, []), State};
  
handle_call({write_key, Offset, Key}, _From, State = #state{keys=Keys}) ->
  {ok, Position} = file:position(Keys, Offset),
  {reply, {file:write(Keys, Key ++ [0]), Position}, State}.

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
terminate(_Reason, #state{index=Index,keys=Keys}) ->
  % error_logger:info_msg("shutting down and closing~n"),
  ok = file:close(Index),
  ok = file:close(Keys).

%%--------------------------------------------------------------------
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @doc Convert process state when code is changed
%% @end 
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------

int_read_key(Keys, [0 | Key]) ->
  lists:reverse(Key);

int_read_key(Keys, Key) ->
  case file:read(Keys, 1) of
    {ok, [Char]} -> int_read_key(Keys, [Char|Key]);
    Other -> Other
  end.

int_read_free(Keys, [0|Bytes]) ->
  Bin = list_to_binary(lists:reverse(Bytes)),
  deserialize(Bin);
  
int_read_free(Keys, Key) ->
  case file:read(Keys, 1) of
    {ok, [Char]} -> int_read_key(Keys, [Char|Key]);
    Other -> Other
  end.
  
deserialize(Bin) when byte_size(Bin) < 8 ->
  {byte_size(Bin), eos};
  
deserialize(<<NextPtr:64, Rest/binary>>) ->
  {byte_size(Rest) + 8, NextPtr}.
  
d_from_blocksize(BlockSize) ->
  trunc((BlockSize - 17)/16).

blocksize_from_d(D) ->
  trunc(16*D + 17).