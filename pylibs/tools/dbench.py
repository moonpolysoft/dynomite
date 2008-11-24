#!/usr/bin/env python

import os
import sys
import pdb

ROOT = os.path.dirname(
    os.path.dirname(
    os.path.abspath(__file__)))
sys.path.append(ROOT)

from dynomite import Client

from optparse import OptionParser
from threading import Thread
from Queue import Queue

from time import time
from random import choice

ports = [11222]


def main():
    rq = Queue()
    results = {'requests': 0, 'get': [], 'put': []}
    options, junk = opts()
    workers = []
    clients = int(options.clients)
    if clients > 1:
        for i in range(0, clients):
            t = Thread(target=run, args=(int(options.number), rq,
                                         int(options.keysize),
                                         int(options.valuesize)))
            workers.append(t)
        for w in workers:
            w.start()
        for w in workers:
            w.join()
            consolidate(rq.get(), results)
            print ".",
    else:
        try:
            consolidate(run(int(options.number), None, int(options.keysize),
                            int(options.valuesize)),
                        results)
        except:
            pdb.post_mortem(sys.exc_info()[2])
        # return
    total_time = 0.0
    for i in results['get']:
        total_time += i
    for i in results['put']:
        total_time += i

    print
    print "%s client(s) %s request(s) %f0.3s" % (options.clients,
                                                 options.number,
                                                 total_time)
    g = results['get']
    g.sort()
    p = results['put']
    p.sort()
    print "get avg: %f0.3ms mean: %f0.3ms 99.9: %f0.3ms" % (
        (sum(g) / float(len(g))) * 1000,
        (g[len(g)/2]) * 1000,
        (g[int(len(g) * .999) -1]) * 1000)
    print "put avg: %f0.3ms mean: %f0.3ms 99.9: %f0.3ms" % (
        (sum(p) / float(len(p))) * 1000,
        (p[len(p)/2]) * 1000,
        (p[int(len(p) * .999) -1]) * 1000)

def run(num, rq, ks, vs):
    res = {'requests': 0,
           'get': [],
           'put': []}

    keys = "abcdefghijklmnop"
    
    client = Client('localhost', choice(ports))
    client.connect()
    
    for i in range(0, num):
        tk = 0.0
        key = ''.join([choice(keys) for i in range(0, ks)])
        st = time()
        cur = client.get(key)
        if cur is None:
            context = ''
        else:
            context = cur[0]
        tk += time() - st
        res['get'].append(tk)
        newval = rval(vs)
        st = time()
        client.put(key, newval, context)
        tk += time() - st
        res['requests'] += 1
        res['put'].append(tk)
    if rq is not None:
        rq.put(res)
    else:
        return res


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
    parser.add_option('-k', '--keysize', default='1',
                      dest='keysize', action='store',
                      help='Length of each key')
    parser.add_option('-v', '--valuesize', default='1024',
                      dest='valuesize', action='store',
                      help='Length of each value')

    return parser.parse_args()


def rval(bsize=1024):
    b = []
    for i in range(0, bsize):
        b.append(choice("abcdefghijklmnopqrstuvwxyz0123456789"))
    return ''.join(b)


if __name__ == '__main__':
    main()
