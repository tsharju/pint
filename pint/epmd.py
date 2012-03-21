import struct
import logging

from functools import wraps

from gevent import Greenlet
from gevent import socket

EPMD_DEFAULT_HOST = 'localhost'
EPMD_DEFAULT_PORT = 4369


def check_response_code(f):
    @wraps(f)
    def wrapper(self, resp_code, socket):
        if resp_code != self.resp_code:
            raise EpmdRequestError("Wrong response code. " + \
                                       "Got '%d', was expecting '%d'")
        return f(self, resp_code, socket)
    return wrapper


class EpmdRequest(object):
    """Base class for requests sent to Erlang Port Mapper Daemon."""

    def encode(self):
        return struct.pack('!H%ds' % len(self.data),
                           len(self.data), self.data)


class EpmdAlive2Request(EpmdRequest):
    """``ALIVE2_REQ`` that can be sent to the EPMD."""

    format = '!BHBBHHH%dsH%ds'
    req_code = 120
    resp_code = 121

    def __init__(self, port, nodename, extra=''):
        fmt = self.format % (len(nodename), len(extra))
        self.data = struct.pack(fmt, self.req_code, port, 72, 0, 5, 5,
                                len(nodename), nodename, len(extra), extra)

    @check_response_code
    def unpack_response(self, resp_code, socket):
        return struct.unpack('!BH', socket.recv(3))


class EpmdPortPleaseRequest(EpmdRequest):
    """``PORT_PLEASE2_REQ`` that can be sent to the EPMD."""

    format = '!B%ds'
    req_code = 122
    resp_code = 119

    def __init__(self, nodename):
        fmt = self.format % len(nodename)
        self.data = struct.pack(fmt, self.req_code, nodename)

    @check_response_code
    def unpack_response(self, resp_code, socket):
        result = struct.unpack('!B', socket.recv(1))[0]
        if result == 0:
            result_data = struct.unpack('!HBBHH', socket.recv(8))
            name_len = struct.unpack('!H', socket.recv(2))[0]
            nodename = struct.unpack('!%ds' % name_len,
                                     socket.recv(name_len))
            extra_len = struct.unpack('!H', socket.recv(2))[0]
            extra = struct.unpack('!%ds' % extra_len,
                                  socket.recv(extra_len))
            return tuple(list(result_data) + list(nodename) + list(extra))


class EpmdConnection(Greenlet):
    """Base class for connections to Erlang Port Mapper Daemon."""

    def __init__(self, epmd_host=EPMD_DEFAULT_HOST,
                 epmd_port=EPMD_DEFAULT_PORT):
        super(EpmdConnection, self).__init__()

        self.epmd_host = epmd_host
        self.epmd_port = epmd_port

        self._connected = True
        self._socket = socket.create_connection((epmd_host, epmd_port))

    def send_request(self, request):
        self._socket.send(request.encode())
        resp_code = struct.unpack('!B', self._socket.recv(1))[0]
        return request.unpack_response(resp_code, self._socket)


class EpmdAliveConnection(EpmdConnection):
    """Registers a new node to the Erlang Port Mapper Daemon with given
    ``nodename`` and ``port``.

    This is done by sending ``ALIVE2_REQ`` to EPMD and leaving the connection
    open. When this connection gets terminated, the node is unregistered from
    EPMD.
    """
    def __init__(self, nodename, port,
                 epmd_host=EPMD_DEFAULT_HOST, epmd_port=EPMD_DEFAULT_PORT):
        super(EpmdAliveConnection, self).__init__(epmd_host, epmd_port)

        self.nodename = nodename
        self.port = port

    def _run(self):
        result, creation = self.send_request(
            EpmdAlive2Request(self.port, self.nodename))
        if result == 0:
            logging.info('Registered node "%s" with ' % self.nodename + \
                             'port %d to EPMD at "%s:%d"' % (self.port,
                                                             self.epmd_host,
                                                             self.epmd_port))
            while self._connected:
                pass
        else:
            logging.error('Could not connect to EPMD at "%s:%d"' \
                              % (self.epmd_host, epmd_port))


def port_please(nodename):
    """Sends a ``PORT_PLEASE2_REQ`` to Erlang Port Mapper Daemon."""
    conn = EpmdConnection()
    return conn.send_request(EpmdPortPleaseRequest(nodename))
