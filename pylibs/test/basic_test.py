def test_client_class_exists():
    from dynomite import Client


def test_client_class_api_methods_exist():
    from dynomite import Client
    Client.connect
    Client.close
    Client.get
    Client.put
    Client.has_key
    Client.delete


def test_client_init_args():
    from dynomite import Client
    Client('localhost', 11222)


class TestWithMockServer(object):

    def setup(self):
        from dynomite import Client
        self.client = self._mock(Client)

    def test_get_not_found_returns_none(self):
        c = self.client
        assert c.get('a') is None

    def test_put_returns_true_value_when_successful(self):
        c = self.client
        assert c.put('a', 'a value')

    def test_put_then_get_returns_value(self):
        c = self.client
        assert c.put('a', 'a value')
        context, values = c.get('a')
        assert context
        assert values == ['a value']

    def _mock(self, client):
        c = client('localhost', 11222)
        c._socket = MockSocket()
        return c


class MockSocket(object):

    def __init__(self):
        self._db = {}

    def send(self, cmd):
        cmd = cmd[:-1] # trim \n
        cmd, rest = cmd.split(' ', 1)
        method = "do_%s" % cmd
        print rest.split(' ')
        return getattr(self, method)(*rest.split(' '))

    def do_get(self, keylen, key):
        keylen = int(keylen)
        if key in self._db:
            self.response = self.get_result(self._db[key])
        else:
            self.response = 'not_found\n'

    def do_put(self, keylen, key, ctxlen, ctx, valuelen, *value):
        value = ' '.join(value)
        val = self._db.setdefault(key, ['', []])
        if val[0] == ctx:
            val[1] = []
        if ctx == '':
            ctx = 'blah'
        val[0] = ctx
        val[1].append(value)
        self.response = 'succ 1\n'

    def recv(self, bytes):
        rsp = self.response[:bytes]
        self.response = self.response[bytes:]
        print rsp, self.response
        return rsp

    def get_result(self, value):
        context, values = value
        items = len(values)
        return 'succ %d %d %s %s\n' % (items, len(context), context,
                                       ' '.join('%d %s' % (len(v), v)
                                                for v in values))
