-ifdef(PROF).
-define(prof(Label),
        dynomite_prof:call(Label, ?MODULE, ?LINE)).
-else.
-define(prof(Label), true).
-endif.
