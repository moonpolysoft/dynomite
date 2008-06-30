-include_lib("include/eunit/eunit.hrl").

split_next_space_test() ->
	{<<"one">>, <<"two">>} = split_next_space(<<"one two">>).

parse_command_test() ->
	{get, <<"my_awesome_key">>} = parse_command(<<"get my_awesome_key">>),
	{set, <<"my_awesome_key">>, <<"my_awesome_data">>} =
		parse_command(<<"set my_awesome_key my_awesome_data">>).