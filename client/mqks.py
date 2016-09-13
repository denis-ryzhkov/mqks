#!/usr/bin/env python

"""
Client of "mqks" - Message Queue Kept Simple.
"""

### import

from adict import adict
from critbot import crit
from functools import partial
from gevent import socket, spawn
import logging
import time
from uqid import uqid

### config

config = adict(
    host='127.0.0.1',
    port=54321,
    logger_name='mqks.client',
    ping_seconds=15,
    reconnect_seconds=1,
    id_length=24,
)

### state

_on_msg = {}
_on_disconnect = {}

### connect

def connect():
    """
    Connect
    """
    while 1:
        try:
            config._log = logging.getLogger(config.logger_name)
            config._log.info('connecting')
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((config.host, config.port))
            config._sock = sock
            spawn(_recv)
            if not config.get('_ping'):
                config._ping = spawn(_ping)
            break

        except Exception:
            crit()
            time.sleep(config.reconnect_seconds)

### publish

def publish(event, data):
    """
    Publish message
    @param event: str
    @param data: str
    """
    _send(_request_id(), 'publish {} {}'.format(event, data))

### subscribe

def subscribe(queue, *events):
    """
    Subscribe to events
    @param queue: str
    @param events: tuple(str)
    """
    _send(_request_id(), 'subscribe {} {}'.format(queue, ' '.join(events)))

### consume

def consume(queue, on_msg, on_disconnect=None, manual_ack=False):
    """
    Consume messages
    @param queue: str
    @param on_msg: callable
    @param on_disconnect: callable
    @param manual_ack: bool
    """
    consumer_id = _request_id()

    _on_msg[consumer_id] = on_msg
    if on_disconnect:
        _on_disconnect[consumer_id] = on_disconnect

    _send(consumer_id, 'consume {}{}'.format(queue, ' --manual-ack' if manual_ack else ''))
    return consumer_id

### ack

def ack(consumer_id, msg_id):
    """
    Ack message
    @param consumer_id: str
    @param msg_id: str
    """
    _send(_request_id(), 'ack {} {}'.format(consumer_id, msg_id))

### ack all

def ack_all(consumer_id):
    """
    Ack all messages
    @param consumer_id: str
    """
    _send(_request_id(), 'ack {} {}'.format(consumer_id, '--all'))

### reject

def reject(consumer_id, msg_id):
    """
    Reject message
    @param consumer_id: str
    @param msg_id: str
    @return:
    """
    _send(_request_id(), 'reject {} {}'.format(consumer_id, msg_id))

### reject all

def reject_all(consumer_id):
    """
    Reject all messages
    @param consumer_id: str
    """
    _send(_request_id(), 'reject {} {}'.format(consumer_id, '--all'))

### delete consumer

def delete_consumer(consumer_id):
    """
    Delete consumer
    @param consumer_id: str
    """
    _on_msg.pop(consumer_id, None)
    _on_disconnect.pop(consumer_id, None)
    _send(_request_id(), 'delete_consumer {}'.format(consumer_id))

### delete queue

def delete_queue(queue, when_unused=False):
    """
    Delete queue
    @param queue: str
    @param when_unused: bool|float|int -
        when_unused=False - Delete queue instantly.
        when_unused=True - Schedule delete when queue is unused by consumers.
        when_unused=5 - Schedule delete when queue is unused by consumers for 5 seconds.
    """
    when_unused = '' if when_unused is False else ' --when-unused{}'.format('' if when_unused is True else '={}'.format(when_unused))
    _send(_request_id(), 'delete_queue {}{}'.format(queue, when_unused))

### ping

def ping(data=None):
    """
    Ping server
    @param data: str
    """
    _send(_request_id(), 'ping {}'.format(data or config.logger_name))

### _ping

def _ping():
    while 1:
        try:
            time.sleep(config.ping_seconds)
            if config.ping_seconds:
                ping()
        except Exception as e:
            crit()

### request id

def _request_id():
    """
    Generate request id
    @return: str
    """
    return uqid(config.id_length)

### _send

def _send(request_id, msg):
    """
    Send request
    @param request_id: str
    @param msg: str
    """

    config._log.debug('#{} > {}'.format(request_id, msg))

    try:
        config._sock.sendall('{} {}\n'.format(request_id, msg))
    except Exception as e:
        crit()

### _recv

def _recv():
    """
    Receive responses
    """
    try:
        sock = config._sock

        f = sock.makefile('r')
        while 1:
            # 1/0  # Test for "on_disconnect".
            response = f.readline()
            if response == '':  # E.g. socket is broken.
                break

            try:
                response = response.rstrip()
                request_id, response_type, data = response.split(' ', 2)
                config._log.debug('#{} < {} {}'.format(request_id, response_type, data))

                if response_type == 'error':
                    raise Exception(response)

                on_msg = _on_msg.get(request_id)
                if on_msg:
                    msg_id, props, data = data.split(' ', 2)

                    msg = adict(
                        id=msg_id,
                        data=data,
                        ack=partial(ack, request_id, msg_id),
                        reject=partial(reject, request_id, msg_id),
                    )

                    for prop in props.split(','):
                        name, value = prop.split('=', 1)
                        msg[name] = value

                    spawn(_safe_on_msg, on_msg, msg)

            except Exception:
                crit(also=response)

    except Exception:
        crit()

    finally:

        for consumer_id, on_disconnect in _on_disconnect.items():
            try:
                on_disconnect()
            except Exception:
                crit(also=consumer_id)

        time.sleep(config.reconnect_seconds)
        connect()

### _safe_on_msg

def _safe_on_msg(on_msg, msg):
    try:
        on_msg(msg)
    except Exception:
        crit()

### tests

def _tests():

    ### import

    import gevent.monkey
    gevent.monkey.patch_all()

    from gevent.event import Event
    import sys

    import critbot.plugins.syslog
    from critbot import crit_defaults
    crit_defaults.plugins = [critbot.plugins.syslog.plugin(logger_name=config.logger_name, logger_level=logging.DEBUG)]

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
            # config._log.info('got {}'.format(msg))
            # msg.ack()

            if not state.start:
                state.start = time.time()

            if msg.data == '1':
                state.stop = time.time()
                done.set()

        def on_disconnect():
            config._log.info('on_disconnect!')

        subscribe('q1', 'user_updated')
        consumer_id = consume('q1', on_msg, on_disconnect=on_disconnect)  # manual_ack=True
        delete_queue('q1', when_unused=True)
        done.wait()
        delete_consumer(consumer_id)

        print(state.stop - state.start)

if __name__ == '__main__':
    _tests()
