def test_client_class_exists():
    from dynomite import Client


def test_client_class_api_methods_exist():
    from dynomite import Client
    Client.connect
    Client.close
    Client.get
    Client.put
    Client.has
    Client.remove


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

    def test_api_calls_work(self):
        c = self.client
        has, count = c.has('the_key')
        assert not has
        assert c.put('the_key', 'a value\ncontaining newlines')
        has, count = c.has('the_key')
        assert has
        context, values = c.get('the_key')
        assert values == ['a value\ncontaining newlines']
        assert c.remove('the_key')
        has, count = c.has('the_key')
        assert not has
        assert c.get('the_key') is None

    def test_get_multiple_values(self):
        c = self.client
        assert c.put('a', 'a value')
        assert c.put('a', 'more values', 'foo')
        context, values = c.get('a')
        assert values == ['a value', 'more values']

    def test_real_world_values(self):
        c = self.client
        assert c.put('j', '4sovqtt9f2y6pkqspzt9rsult2qa1geem636wztvu0t0aawgz7okxaqepx6uy5oehku2n01rvwtc6mtmufkmddg0c7rdasba1tpqt8hnyed49q49kysqdrsxwuqe4apdp4scc2kkajuyltnhpdwjylzmxpgtapd9d5raw7wzepxg7kv93j4y0v7lrv8wwysaxf01vwcmh7yt17qpp5mq5ojxvx6x32dkzhbx6r97529cc6lgr7e85u9gv32mfjzlzmarbjmufrdrzdov222wsdd5ppkrl8iiry93n36a11s3ml527v1psxq8q3jeihrgynldh3mclif30ddo54m888p8kkd1uj8qrdeajkbldqzzazs05neuxwx9d1kj40aehntgwbq2fqmt4pxgtrn0wmje7tvu86f5ebb01hgijlz3mlvqpa2ru8129cz08ydy1tx33kkveel8xrurin9arj8okblq9kuvsgpwmtf36gj9x138oai6278fbf6xu5fen3f7eriq9qd00t0c858nwqm2bb5690e3yuyi2rr4raz66nyloxxx81zg0cafh6rcg6qpta6dic49p5ndp9q2nudaa434lc4poc0keruju4q70lj994s4f7muzr33pbb9hd0h77xpkod09eqsxsgcj1tj1h3djdhvwfq829t9uh4pp4tln8q3wlp6ptrcz7tlp4xk0t6vyu52hnfatfjlcvm2nfcx0kttqzsyfg6fgz0bbo9hegqiv043kbyujiwqs1v5jx2pht10c4au3ga4daadifojtnni2nre8rcf0ppnkkboia7jhaut4hs83djzdgwq51pg05wy66v3ipr4u6m64l1jy9s93goegwsep57n435kkw4cbuyuotu55ge6ihbzdb0z67qhmbis1ntyj92rqmsehxoz0m1skp6nxmwgsaqikenz8isr57oh3acr1nqxqnn97t7vj4x64lv5uahp6z5498s58t076ovo93myhpubjux0r8xldgne4tfj', '\x83l\x00\x00\x00\x01h\x02d\x00\x0ct1@localhostc1.22730354685385799408e+09\x00\x00\x00\x00\x00j')
        context, values = c.get('j')
        assert context == '\x83l\x00\x00\x00\x01h\x02d\x00\x0ct1@localhostc1.22730354685385799408e+09\x00\x00\x00\x00\x00j'
        assert values == ['4sovqtt9f2y6pkqspzt9rsult2qa1geem636wztvu0t0aawgz7okxaqepx6uy5oehku2n01rvwtc6mtmufkmddg0c7rdasba1tpqt8hnyed49q49kysqdrsxwuqe4apdp4scc2kkajuyltnhpdwjylzmxpgtapd9d5raw7wzepxg7kv93j4y0v7lrv8wwysaxf01vwcmh7yt17qpp5mq5ojxvx6x32dkzhbx6r97529cc6lgr7e85u9gv32mfjzlzmarbjmufrdrzdov222wsdd5ppkrl8iiry93n36a11s3ml527v1psxq8q3jeihrgynldh3mclif30ddo54m888p8kkd1uj8qrdeajkbldqzzazs05neuxwx9d1kj40aehntgwbq2fqmt4pxgtrn0wmje7tvu86f5ebb01hgijlz3mlvqpa2ru8129cz08ydy1tx33kkveel8xrurin9arj8okblq9kuvsgpwmtf36gj9x138oai6278fbf6xu5fen3f7eriq9qd00t0c858nwqm2bb5690e3yuyi2rr4raz66nyloxxx81zg0cafh6rcg6qpta6dic49p5ndp9q2nudaa434lc4poc0keruju4q70lj994s4f7muzr33pbb9hd0h77xpkod09eqsxsgcj1tj1h3djdhvwfq829t9uh4pp4tln8q3wlp6ptrcz7tlp4xk0t6vyu52hnfatfjlcvm2nfcx0kttqzsyfg6fgz0bbo9hegqiv043kbyujiwqs1v5jx2pht10c4au3ga4daadifojtnni2nre8rcf0ppnkkboia7jhaut4hs83djzdgwq51pg05wy66v3ipr4u6m64l1jy9s93goegwsep57n435kkw4cbuyuotu55ge6ihbzdb0z67qhmbis1ntyj92rqmsehxoz0m1skp6nxmwgsaqikenz8isr57oh3acr1nqxqnn97t7vj4x64lv5uahp6z5498s58t076ovo93myhpubjux0r8xldgne4tfj']
        
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

    def do_has(self, keylen, key):
        if key in self._db:
            self.response = 'yes 1\n'
        else:
            self.response = 'no 1\n'

    def do_del(self, keylen, key):
        try:
            self._db.pop(key)
        except KeyError:
            pass
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
