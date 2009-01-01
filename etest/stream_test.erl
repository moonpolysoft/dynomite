-include_lib("eunit.hrl").

-define(MEG, 1048576).

simple_streaming_test() ->
  Bits = ?MEG*8,
  Bin = <<0:Bits>>,
  ?MEG = byte_size(Bin),
  Ref = make_ref(),
  Parent = self(),
  process_flag(trap_exit, true),
  Receiver = spawn_link(fun() -> 
      receive
        {Ref, Sender} -> 
          Results = recv(Sender, 200),
          Parent ! {Ref, Results}
      end
    end),
  Sender = spawn_link(fun() ->
      reply(Receiver, {context, [Bin]})
    end),
  Receiver ! {Ref, Sender},
  {ok, {context, [Bin]}} = receive
    {Ref, Results} -> Results
  end,
  receive
    {'EXIT', Receiver, _} -> ok
  end,
  receive
    {'EXIT', Sender, _} -> ok
  end.