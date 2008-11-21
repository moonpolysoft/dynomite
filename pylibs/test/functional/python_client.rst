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
    >>> c.has_key('a')
    (True, 1)
    >>> c.delete('a')
    1
    >>> c.has_key('a')
    (False, 1)
    >>> c.get('a')

