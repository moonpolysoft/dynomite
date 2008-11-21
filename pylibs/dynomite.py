import socket
from threading import local

class DynomiteError(IOError):
    pass


class Client(local):

    def __init__(self, host, port):
        self._host = host
        self._port = port
        self._socket = None
    
    def connect(self):
        if self._socket:
            return
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.connect((self._host, self._port))

    def close(self):
        if not self._socket:
            return
        self._socket.close()
        self._socket = None

    def get(self, key):
        """
        Get the value(s) for a key. Returns None, or a 2-tuple of
        (context, values) where values is a list of the values currently
        known for the key. If most cases, the list will have only one item.
        In cases of a write conflict or an update that has not fully
        propagated, there will be more than one item in the list, which you
        must resolve according to the appropriate logic of your application.
        """
        # get command line:
        # get $keylength $key\n
        # result:
        # fail $reason\n
        # not_found\n
        # succ $items $ctx_length $ctx ($data_length $data)+\n
        self.connect()
        self._socket.send("get %d %s\n" % (len(key), key))
        return self._get_result()

    def put(self, key, value, context=''):
        """
        Put the given value for the given key into dynomite. The submitted
        context should be one returned from a previous get.

        FIXME: value and context must be strings. In reality, should
        pickle any value that is not a string.
        """
        # put command line:
        # put $keylength $key $ctx_length $ctx $data_length $data\n
        # result:
        # fail $reason\n
        # succ $num_servers_stored\n
        self.connect()
        self._socket.send("put %d %s %d %s %d %s\n" %
                          (len(key), key,
                           len(context), context,
                           len(value), value))
        return self._update_result()

    def has_key(self, key):
        """
        Check that the given key exists in the key store.
        """
        # has_key command line:
        # has $keylen $key
        # result:
        # fail $reason
        # yes $num_servers_responding
        # no $num_servers_responding
        self.connect()
        self._socket.send('has %d %s\n' % (len(key), key))
        return self._has_key_result()
        
    def delete(self, key):
        """
        Delete the given key from the key store.
        """
        # delete command line:
        # del $keylength $key
        # result:
        # fail $reason
        # succ $num_servers_responding
        self.connect()
        self._socket.send('del %d %s\n' % (len(key), key))
        return self._update_result()

    def _get_result(self):
        cmd = self._read_command()
        if cmd == 'not_found':
            return None
        elif cmd == 'succ':
            item_len = int(self._read_section())
            ctx_len = int(self._read_section())
            ctx = self._read_bin(ctx_len)
            items = []
            for i in range(0, item_len):
                data_len = int(self._read_section())
                # FIXME: unpickle values
                items.append(self._read_bin(data_len))
            return (ctx, items)
        else:
            raise IOError("Unexpected response: %s" % cmd)

    def _update_result(self):
        cmd = self._read_command()
        if cmd == 'succ':
            return int(self._read_section())
        else:
            raise IOError("Unexpected response: %s" % cmd)

    def _has_key_result(self):
        cmd = self._read_command()
        try:
            count = int(self._read_section())
        except ValueError:
            raise IOError("Unexpected response to has_key")
        return (cmd == 'yes', count)

    def _read_command(self):
        self._buf = ''
        cmd = self._read_section()
        if cmd == 'fail':
            reason = self._read_line()
            raise DynomiteError(reason)
        return cmd
    
    def _read_section(self, eol=False):
        buf = self._buf
        recv = self._socket.recv
        while True:
            if not eol:
                index = buf.find(' ')
                if index > 0:
                    break
            index = buf.find('\n')
            if index > 0:
                break
            data = recv(4096)
            # print 'data received', data
            if not data:
                break
            buf += data
        if index > 0:
            self._buf = buf[index+1:]
            buf = buf[:index]
        else:
            self._buf = ''
        # print 'read section result "%s"' % buf
        # print 'remaining buffer "%s"' % self._buf
        return buf

    def _read_line(self):
        return self._read_section(eol=True)

    def _read_bin(self, bytes):
        buf = self._buf
        recv = self._socket.recv
        while len(buf) < bytes:
            chunk = recv(4096)
            if len(chunk) == 0:
                raise IOError("Read only %d bytes of %d" % (len(buf), butes))
            buf += chunk
        self._buf = buf[bytes+1:] # skip terminator
        return buf[:bytes]
