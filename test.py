import sys
sys.path.append("gen-py")
from dynomite import Dynomite
from dynomite.ttypes import *

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

# Make socket
transport = TSocket.TSocket('localhost', 9200)

# Buffering is critical. Raw sockets are very slow
transport = TTransport.TBufferedTransport(transport)

# Wrap in a protocol
protocol = TBinaryProtocol.TBinaryProtocol(transport)

client = Dynomite.Client(protocol)
transport.open()

# print client.put("b", None, "b value")
# rb =  client.get("b")
# client.put("b", rb.context, "electric b-value monkey fish!")

# r = client.get("a")
# print r

# print client.put("a", r.context, "a new value")
# print client.get("a")

# print client.put("a", r.context, "a newer new value")

# print client.get("a")
# print client.get("b")


print client.get("g")

transport.close()
