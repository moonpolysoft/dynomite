----------------------
Dynomite Python Client
----------------------

FIXME: explain

Basic usage
===========

    >>> from dynomite import Client
    >>> c = Client('localhost', 11222)
    >>> c.get('a')
    >>> c.put('a', 'a value')
    1
    >>> c.get('a') # doctest: +ELLIPSIS
    ('...', ['a value'])
    >>> c.has('a')
    (True, 1)
    >>> c.remove('a')
    1
    >>> c.has('a')
    (False, 1)
    >>> c.get('a')

Thrift
======

    >>> from dynomite.thrift_client import Client
    >>> c = Client('localhost', 9200)
    >>> c.get('b').results
    []
    >>> c.put('b', 'b value')
    1
    >>> c.get('b').results
    ['b value']
    >>> c.remove('b')
    1
    >>> c.get('b').results
    []
