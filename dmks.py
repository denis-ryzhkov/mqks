#!/usr/bin/env python

"""
Direct Messages Kept Simple.

Features:
* P2P LAN TCP.
* Minimal number of multiplexed sockets.
* Up to 4 GiB key-value.
* High performance:
      1 GiB in 1.272 seconds
    100 MiB in 0.146 seconds
     10 MiB in 0.018 seconds
      1 MiB in 0.005 seconds
      1 KiB in 0.003 seconds

Usage:
* Each party calls "dmks.init()".
* Receiver spawns passing of its "dmks.address()" and any "key" to sender via MQ, DB, etc.
* Receiver calls "value = dmks.recv(key)" and blocks.
* Sender calls "dmks.send(address, key, value)".
* Receiver is unblocked, done.
"""

__all__ = ['init', 'address', 'recv', 'send']

### import

from critbot import crit
import errno
from gevent import spawn
from gevent.event import AsyncResult
from gevent.queue import Queue
from gevent.server import StreamServer
import io
import socket
import struct
import time

### state

state = dict(
    # address: tuple(host: str, port: int),
    # server: StreamServer,
)

clients = dict()  # clients[address: tuple] == socket
outbox = Queue()  # outbox.get() == tuple(address: tuple, key: str, value: str)
results = dict()  # results[key: str] == result: AsyncResult, result.get() == value: str

SIZE_FORMAT = '!I'                          # network unsigned int
SIZE_SIZE = struct.calcsize(SIZE_FORMAT)    # 4 bytes
MAX_SIZE = 2 ** (8 * SIZE_SIZE) - 1         # 4 GiB minus 1 byte

### init

def init(host):
    """
    Init DMKS.

    @param host: str - LAN IP, e.g. mqks.config['_sock'].getsockname()[0]
    """
    server = StreamServer((host, 0), on_client)  # 0 = Random available port.
    server.init_socket()
    port = server.socket.getsockname()[1]
    state.update(address=(host, port), server=server)

    spawn(receiver)
    spawn(sender)

### address

def address():
    """
    Get local DMKS address.

    @return tuple(host: str, port: int)
    """
    return state['address']

### recv

def recv(key, timeout=10):
    """
    Receive a value for this "key".

    @param key: str
    @param timeout: float|None - How many seconds to wait. Enables "gevent.timeout.Timeout" excepton.
    @return str
    """
    results[key] = AsyncResult()
    try:
        return results[key].get(timeout=timeout)
    finally:
        del results[key]

### send

def send(address, key, value):
    """
    Send a "value" for "key" to "address".
    Sender should get these params from receiver via MQ, DB, etc.

    @param address: tuple(host: str, port: int)
    @param key: str - Less than 4 GiB.
    @param value: str - Less than 4 GiB.
    """
    host, port = address
    assert isinstance(host, basestring), address
    assert isinstance(port, int), address
    address = (host, port)  # Tuple may become "list" while traveling via JSON, we need "tuple".
    assert len(key) <= MAX_SIZE, len(key)
    assert len(value) <= MAX_SIZE, len(value)
    outbox.put((address, key, value))

### sender

def sender():
    while 1:
        try:
            address, key, value = outbox.get()

            sock = clients.get(address)
            if not sock:
                sock = socket.socket()
                sock.connect(address)
                clients[address] = sock

            try:
                sock.sendall(struct.pack(SIZE_FORMAT, len(key)))
                sock.sendall(key)
                sock.sendall(struct.pack(SIZE_FORMAT, len(value)))
                sock.sendall(value)

            except Exception:
                del clients[address]  # Reconnect next time.
                raise

        except Exception:
            crit()

### receiver

def receiver():
    try:
        state['server'].serve_forever()
    except Exception:
        crit()

### on_client

def on_client(sock, address):
    try:
        while 1:
            size, = struct.unpack(SIZE_FORMAT, recvall(sock, SIZE_SIZE))
            key = recvall(sock, size)
            size, = struct.unpack(SIZE_FORMAT, recvall(sock, SIZE_SIZE))
            value = recvall(sock, size)

            result = results.get(key)
            if result:
                result.set(value)

    except Exception as e:
        if not is_disconnect(e):
            crit(also=dict(address=address))

### is_disconnect

def is_disconnect(e):
    e = repr(e)
    return 'Connection reset by peer' in e or 'Broken pipe' in e or 'Bad file descriptor' in e

### recvall

def recvall(sock, nbytes):
    buffer = io.BytesIO()
    while nbytes:
        chunk = sock.recv(nbytes)
        if not chunk:
            raise socket.error(errno.ECONNRESET, 'Connection reset by peer')
        buffer.write(chunk)
        nbytes -= len(chunk)
    return buffer.getvalue()

### tests

def tests():
    import gevent.monkey
    gevent.monkey.patch_all()

    import critbot.plugins.syslog, logging
    from critbot import crit_defaults
    crit_defaults.plugins = [critbot.plugins.syslog.plugin(logger_name='dmks', logger_level=logging.INFO)]

    init('127.0.0.1')

    key = 's' * 24
    value = 's' * 1 * 1024 * 1024 * 1024
    print(len(value))

    spawn(send, address(), key, value)

    start = time.time()
    received_value = recv(key)
    print('{:.6f}'.format(time.time() - start))  # See "Performance".

    assert received_value == value

if __name__ == '__main__':
    tests()
