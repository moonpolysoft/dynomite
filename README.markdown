NOTICE
========

If the lack of commits over the past year isn't a clue, this is your warning that Dynomite is a dead project and is no longer
being maintained.  At some point in the future my employer may allow me to push out the improvements that I've made
since I was barred from pushing code publicly.  But I would not count on it, and I would not recommend you use Dynomite
for anything other than a functional design document on how to build a Dynamo clone.

If you need an erlang Dynamo clone for production use I would recommend [Riak](http://riak.basho.com/).

If you need something with a bigtable style data model then I recommend [Cassandra](http://cassandra.apache.org/).



Dynomite
-------

This is dynomite.  It is a clone of the amazon dynamo key value store written in Erlang.

[Amazon's Dynamo](http://www.allthingsdistributed.com/2007/10/amazons_dynamo.html)

Documentation
---

* [Introduction](http://wiki.github.com/cliffmoon/dynomite/home)
* [Getting Started](http://wiki.github.com/cliffmoon/dynomite/getting-started)
* [Clustering Guide](http://wiki.github.com/cliffmoon/dynomite/clustering-guide)

IRC
---

channel #dynomite on irc.freenode.net

Mailing List
---

[Dynomite Mailing List](http://groups.google.com/group/dynomite-users)

TL;DR Getting Started
----

	git clone git://github.com/cliffmoon/dynomite.git
	cd dynomite
	git submodule init
	git submodule update
	rake
	./bin/dynomite start -c config.json
	
