%% This library is free software; you can redistribute it and/or modify
%% it under the terms of the GNU Lesser General Public License as
%% published by the Free Software Foundation; either version 2 of the
%% License, or (at your option) any later version.
%%
%% This library is distributed in the hope that it will be useful, but
%% WITHOUT ANY WARRANTY; without even the implied warranty of
%% MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
%% Lesser General Public License for more details.
%%
%% You should have received a copy of the GNU Lesser General Public
%% License along with this library; if not, write to the Free Software
%% Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307
%% USA
%%
%% $Id:$
%%
%% Copyright (C) 2006 Richard Carlsson

%% Including this file turns off testing unless explicitly enabled by
%% defining TEST or EUNIT. If both NOTEST and TEST (or EUNIT) are
%% defined, then TEST/EUNIT takes precedence, and NOTEST will become
%% undefined.

%% set NOTEST, then read eunit.hrl
-ifndef(NOTEST).
-define(NOTEST, true).
-endif.
%% Since this file is normally included with include_lib, it must in its
%% turn use include_lib to read any other header files, at least until
%% the epp include_lib behaviour is fixed.
-include_lib("eunit/include/eunit.hrl").
%%-include("eunit.hrl").
