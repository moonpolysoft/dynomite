#!/usr/bin/env python

from dynomite.thrift_client import Client

from optparse import OptionParser
from threading import Thread
from Queue import Queue

import time
import random
import sha


def main():
    rq = Queue()
    options, junk = opts()
    workers = []
    host = options.host
    port = int(options.port)
    clients = int(options.clients)
    minsize = int(options.min_value)
    maxsize = int(options.max_value)
    sleep = int(options.sleep)
    verbose = options.verbose
    log = options.log
    
    for i in range(0, clients):
        t = Thread(target=run, args=(rq, host, port,
                                     sleep, minsize, maxsize))
        workers.append(t)
    for w in workers:        
        w.setDaemon(True)
        w.start()

    stats = {'collisions': 0,
             'get': [],
             'put': []}
    while True:
        alive = False
        for w in workers:
            if w.isAlive():
                alive = True
                break
        if not alive:
            break

        r = rq.get()
        stats['collisions'] += r['collisions']
        stats['get'].extend(r['get'])
        stats['put'].extend(r['put'])
        g = stats['get'][:]
        g.sort()
        p = stats['put'][:]
        p.sort()
        gets = len(g)
        puts = len(p)
        print "gets: %d puts: %d collisions: %d" \
              % (gets, puts, stats['collisions'])
        print "get avg: %f0.3ms mean: %f0.3ms 99.9: %f0.3ms" % (
            (sum(g) / float(gets)) * 1000,
            (g[gets/2]) * 1000,
            (g[int(gets * .999)-1]) * 1000)
        print "put avg: %f0.3ms mean: %f0.3ms 99.9: %f0.3ms" % (
            (sum(p) / float(puts)) * 1000,
            (p[puts/2]) * 1000,
            (p[int(puts * .999)-1]) * 1000)

        
def run(rq, host, port, sleep, minsize, maxsize):
    client = Client(host, port)
    k = key()
    while True:
        s = {'collisions': 0,
             'get': [],
             'put': []}
        st = time.time()
        cur = client.get(k)
        taken = time.time() - st
        s['get'].append(taken)
        if len(cur.results) > 1:
            s['collisions'] += 1
        nval = rval(minsize, maxsize)
        st = time.time()
        client.put(k, nval, cur.context)
        taken = time.time() - st
        s['put'].append(taken)
        rq.put(s)
        time.sleep(random.uniform(0, sleep))
    


def opts():
    parser = OptionParser()

    # FIXME num requests / time to run

    parser.add_option('--host', dest='host', default='localhost',
                      action='store', help='Connect to host')
    parser.add_option('-p', '--port', dest='port', default=9200,
                      action='store', help='Connect to port')
    parser.add_option('-s', '--sleep', dest='sleep', default='2',
                      action='store', help='Time to sleep between requests '
                      '(seconds)')
    parser.add_option('-c', '--concurrency', '--clients', default='1',
                      dest='clients', action='store',
                      help='Number of concurrent clients')
    parser.add_option('-m', '--min-value-size', default='1024',
                      dest='min_value', action='store',
                      help='Min length of each value')
    parser.add_option('-x', '--max-value-size', default='4096',
                      dest='max_value', action='store',
                      help='Max length of each value')
    parser.add_option('-l', '--log', default='load_thrift.log',
                      dest='log', action='store',
                      help='Periodically log load stats to this file')
    parser.add_option('--verbose', default=False,
                      dest='verbose', action='store_true',
                      help='Print stats to stdout')

    return parser.parse_args()


def key():
    return sha.new(
        ''.join(random.sample('0123456789abcdefghijklmnop', 10))).hexdigest()


def rval(min, max):
    ch = random.choice
    size = random.randrange(min, max)
    b = []
    for i in range(0, size):
        b.append(ch("abcdefghijklmnopqrstuvwxyz0123456789"))
    return ''.join(b)


if __name__ == '__main__':
    main()
