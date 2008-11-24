from dynomite import Dynomite
from dynomite.ttypes import *

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from threading import local, RLock

class Client(object):
    """
    Simplified thrift client abstracts some thrift mechanics. Presents same
    api as dynomite.Dynomite.Client, adding automatic setup of the transport
    and protocol, and threadsafety.

    """    
    def __init__(self, host, port):
        self._host = host
        self._port = port
        self._lock = RLock()
        self._con = local()

    def get(self, key):
        self.connect()
        return self._con.client.get(key)

    def put(self, key, value, context=''):
        self.connect()
        return self._con.client.put(key, context, value)

    def has(self, key):
        self.connect()
        return self._con.client.has(key)

    def remove(self, key):
        self.connect()
        return self._con.client.remove(key)

    def connect(self):
        if hasattr(self._con, 'client'):
            return
        self._lock.acquire()
        try:
            transport = TSocket.TSocket(self._host, self._port)
            transport = TTransport.TBufferedTransport(transport)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            self._con.client = Dynomite.Client(protocol)
            self._con.transport = transport
            transport.open()
        finally:
            self._lock.release()

    def disconnect(self):
        if not hasattr(self._con, 'transport'):
            return
        self._con.transport.close()
