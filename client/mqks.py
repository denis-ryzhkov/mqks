#!/usr/bin/env python

"""
Client of "mqks" - Message Queue Kept Simple.

@author Denis Ryzhkov <denisr@denisr.com>
"""

### import

from adict import adict
from critbot import crit, crit_defaults
from gevent import socket, spawn
import logging
import time
from uqid import uqid

### config

config = adict(
    name='mqks.client',
    host='127.0.0.1',
    port=54321,
    logger_level=logging.INFO,
    reconnect_seconds=1,
    id_length=24,
)

### state

_on_msg = {}
_on_disconnect = {}

### connect

def connect():
    while 1:
        try:
            config._log = logging.getLogger(config.name)
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((config.host, config.port))
            config._sock = sock
            spawn(_listen)
            break

        except Exception:
            crit()
            time.sleep(config.reconnect_seconds)

### publish

def publish(event, data):
    _send(_request_id(), 'publish {} {}'.format(event, data))

### subscribe

def subscribe(queue, *events):
    _send(_request_id(), 'subscribe {} {}'.format(queue, ' '.join(events)))

### consume

def consume(queue, on_msg, on_disconnect=None, manual_ack=False):
    consumer_id = _request_id()

    _on_msg[consumer_id] = on_msg
    if on_disconnect:
        _on_disconnect[consumer_id] = on_disconnect

    _send(consumer_id, 'consume {}{}'.format(queue, ' --manual_ack' if manual_ack else ''))
    return consumer_id

### ack

def ack(consumer_id, msg_id='--all'):
    _send(_request_id(), 'ack {} {}'.format(consumer_id, msg_id))

### reject

def reject(consumer_id, msg_id='--all'):
    _send(_request_id(), 'reject {} {}'.format(consumer_id, msg_id))

### delete_consumer

def delete_consumer(consumer_id):
    _on_msg.pop(consumer_id, None)
    _on_disconnect.pop(consumer_id, None)
    _send(_request_id(), 'delete_consumer {}'.format(consumer_id))

### delete_queue

def delete_queue(queue, auto=False):
    _send(_request_id(), 'delete_queue {}{}'.format(queue, ' --auto' if auto else ''))

### _request_id

def _request_id():
    return uqid(config.id_length)

### _send

def _send(request_id, msg):
    config._log.debug('#{} > {}'.format(request_id, msg))
    config._sock.sendall('{} {}\n'.format(request_id, msg))

### _listen

def _listen():
    try:
        sock = config._sock

        f = sock.makefile('r')
        while 1:
            #1/0  # Test for "on_disconnect".
            response = f.readline()
            if response == '':
                break

            try:
                response = response.rstrip()
                request_id, response_type, data = response.split(' ', 2)
                config._log.debug('#{} < {} {}'.format(request_id, response_type, data))

                if response_type == 'error':
                    raise Exception(response)

                on_msg = _on_msg.get(request_id)
                if on_msg:
                    id_, event, data = data.split(' ', 2)
                    msg = adict(id=id_, event=event, data=data)
                    spawn(on_msg, msg)

            except Exception:
                crit(also=response)

    except Exception:
        crit()

        for consumer_id, on_disconnect in _on_disconnect.items():
            try:
                on_disconnect()
            except Exception:
                crit(also=consumer_id)

        time.sleep(config.reconnect_seconds)
        connect()

### tests

def _tests():

    ### import

    import gevent.monkey
    gevent.monkey.patch_all()

    from gevent.event import Event
    import sys

    import critbot.plugins.syslog
    crit_defaults.plugins = [critbot.plugins.syslog.plugin(logger_name=config.name, logger_level=config.logger_level)]

    ### main

    connect()

    if 'pub' in sys.argv:

        ### publish

        for data in xrange(30 * 1000, 0, -1):
            publish('user_updated', data)

    else:

        ### consume

        state = adict(start=None, stop=None)
        done = Event()

        def on_msg(msg):
            #config._log.info('got {}'.format(msg))
            #ack(consumer_id, msg.id)

            if not state.start:
                state.start = time.time()

            if msg.data == '1':
                state.stop = time.time()
                done.set()

        def on_disconnect():
            config._log.info('on_disconnect!')

        subscribe('q1', 'user_updated')
        delete_queue('q1', auto=True)
        consumer_id = consume('q1', on_msg, on_disconnect=on_disconnect)  # manual_ack=True
        done.wait()
        delete_consumer(consumer_id)

        print(state.stop - state.start)

if __name__ == '__main__':
    _tests()
