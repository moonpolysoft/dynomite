#!/usr/bin/env python

import sys
sys.path.append("gen-py")
from dynomite import Dynomite
from dynomite.ttypes import *

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from optparse import OptionParser
from threading import Thread
from Queue import Queue

from time import time
from random import choice

ports = [9200] #, 9201, 9202, 9203]


def main():
    rq = Queue()
    results = {}
    options, junk = opts()
    workers = []
    for i in range(0, int(options.clients)):
        t = Thread(target=run, args=(int(options.number), rq))
        workers.append(t)
    for w in workers:
        w.start()
    for w in workers:
        if w.isAlive():
            w.join()
            consolidate(rq.get(), results)
            print ".",
        else:
            print "x",
    print
    print results


def run(num, rq):
    res = {'requests': 0,
           'get': [],
           'put': []}

    keys = "abcdefghijklmnop"

    # Make socket
    transport = TSocket.TSocket('localhost', choice(ports))

    # Buffering is critical. Raw sockets are very slow
    transport = TTransport.TBufferedTransport(transport)

    # Wrap in a protocol
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    
    client = Dynomite.Client(protocol)
    transport.open()

    for i in range(0, num):
        tk = 0.0
        key = choice(keys)
        st = time()
        print "get", key
        cur = client.get(key)
        tk += time() - st
        res['get'].append(tk)
        newval = rval()
        st = time()
        print "put", key
        client.put(key, cur.context, newval)
        tk += time() - st
        res['requests'] += 1
        res['put'].append(tk)
    rq.put(res)


def consolidate(res, results):
    results['requests'] += res['requests']
    results['get'].extend(res['get'])
    results['put'].extend(res['put'])


def opts():
    parser = OptionParser()
    parser.add_option('-n', '--number', dest='number', default='10',
                      action='store', help='Number of requests per client')
    parser.add_option('-c', '--concurrency', '--clients', default='1',
                      dest='clients', action='store',
                      help='Number of concurrent clients')

    return parser.parse_args()


def rval(bsize=1024):
    b = []
    for i in range(0, bsize):
        b.append(choice("abcdefghijklmnopqrstuvwxyz0123456789"))
    return ''.join(b)


if __name__ == '__main__':
    main()
