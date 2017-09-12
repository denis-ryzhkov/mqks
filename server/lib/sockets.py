
### import

from gevent.server import _tcp_listener
from socket import AF_INET, AF_UNIX
import os
import time

from mqks.server.config import config, log
from mqks.server.lib import state

### get_listener

def get_listener(port):
    """
    Get TCP socket listener.

    @param port: int or basestring - int = Internet port, basestring = relative file name of UNIX domain socket
    @return listener: gevent._socket2.socket
    """
    assert isinstance(port, (int, basestring)), (port, type(port))
    family = AF_INET if isinstance(port, int) else AF_UNIX
    if family == AF_INET:
        host = '0.0.0.0' if config['listen_any'] else config['host']
        addr = (host, port)
    else:
        addr = os.path.join(config['unix_sock_dir'], port)

    listener = None
    while 1:
        try:
            if family == AF_UNIX:

                if not os.path.exists(config['unix_sock_dir']):
                    try:
                        os.mkdir(config['unix_sock_dir'])
                    except OSError as e:
                        if 'exists' not in repr(e):  # Racing with other worker.
                            raise

                if os.path.exists(addr):
                    os.remove(addr)

            listener = _tcp_listener(addr, family=family, reuse_addr=True, backlog=config['backlog'])
            break

        except Exception as e:
            if 'Address already in use' not in repr(e):
                raise
            log.error('w{}: {}'.format(state.worker, e))
        time.sleep(config['block_seconds'])

    log.info('w{}: listening {}'.format(state.worker, addr))
    return listener

### is_disconnect

def is_disconnect(e):
    """
    Checks if exception is a disconnect.

    @param e: Exception
    @return bool
    """
    e = repr(e)
    return (
        'Bad file descriptor' in e or
        'Broken pipe' in e or
        'Connection refused' in e or
        'Connection reset by peer' in e or
        'File descriptor was closed in another greenlet' in e or
        'No route to host' in e
   )
