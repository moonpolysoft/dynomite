This is dynomite.  It is a clone of the amazon dynamo key value store written in Erlang.

[Amazon's Dynamo](http://www.allthingsdistributed.com/2007/10/amazons_dynamo.html)

To use:

	git clone git://github.com/cliffmoon/dynomite.git
	cd dynomite
	git submodule init
	git submodule update
	rake
	./bin/dynomite start -m ~/some/data/dir -n 1 -r 1 -w 1
	
[Dynomite Mailing List](http://groups.google.com/group/dynomite-users)