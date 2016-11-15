#!/usr/bin/env python

"""
Client of "mqks" - Message Queue Kept Simple.
"""

### import

from adict import adict
from collections import defaultdict
from critbot import crit
from functools import partial
from gevent import socket, spawn
from gevent.event import Event
from gevent.queue import Queue, Empty
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

_consumers = {}
_on_msg = {}
_on_disconnect = {}
_confirms = defaultdict(Event)
_requests = Queue()

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

            for consumer_id, consumer in _consumers.items():
                _send(consumer_id, 'consume', consumer, async=False)

            if not config.get('_sender'):
                config._sender = spawn(_sender)

            if config.ping_seconds and not config.get('_ping'):
                config._ping = spawn(_ping)

            break

        except Exception:
            crit()
            time.sleep(config.reconnect_seconds)

### publish

def publish(event, data, confirm=False):
    """
    Client publishes new message to server.
    Server puts copies of this message to zero or more queues that were subscribed to this event.

    @param event: str
    @param data: str
    @param confirm: bool
    """
    _send(_request_id(), 'publish', '{} {}'.format(event, data), confirm=confirm)

### consume

def consume(queue, events, on_msg, on_disconnect=None, delete_queue_when_unused=False, manual_ack=False, confirm=False):
    """
    Client starts consuming from queue, subscribed to zero or more events.
    Client may ask server to delete this queue when it is unused by consumers (for some seconds).
    When client disconnects, server deletes all consumers of this client.
    When client reconnects, it restarts all its consumers.
    If consumer with manual-ack disconnects, all not-acked messages are automatically rejected by server - returned to the queue.

    @param queue: str
    @param events: iterable(str)
    @param on_msg: callable
    @param on_disconnect: callable
    @param delete_queue_when_unused: bool|float|int -
        False - Do not delete the queue.
        True - Schedule delete when queue is unused by consumers.
        5 - Schedule delete when queue is unused by consumers for 5 seconds.
    @param manual_ack: bool
    @param confirm: bool
    """

    consumer_id = _request_id()

    _on_msg[consumer_id] = on_msg
    if on_disconnect:
        _on_disconnect[consumer_id] = on_disconnect

    consumer = '{} {}{}{}'.format(
        queue,
        ' '.join(events),
        '' if delete_queue_when_unused is False else ' --delete-queue-when-unused{}'.format(
            '' if delete_queue_when_unused is True else '={}'.format(delete_queue_when_unused)
        ),
        ' --manual-ack' if manual_ack else '',
    )
    _consumers[consumer_id] = consumer

    _send(consumer_id, 'consume', consumer, confirm=confirm)
    return consumer_id

### ack

def ack(consumer_id, msg_id, confirm=False):
    """
    Acknowledge this message was processed by this consumer.

    @param consumer_id: str
    @param msg_id: str
    @param confirm: bool
    """
    _send(_request_id(), 'ack', '{} {}'.format(consumer_id, msg_id), confirm=confirm)

### ack all

def ack_all(consumer_id, confirm=False):
    """
    Acknowledge all messages were processed by this consumer.

    @param consumer_id: str
    @param confirm: bool
    """
    ack(consumer_id, '--all', confirm=confirm)

### reject

def reject(consumer_id, msg_id, confirm=False):
    """
    Reject this message - to return it to the queue with incremented "retry" counter.

    @param consumer_id: str
    @param msg_id: str
    @param confirm: bool
    """
    _send(_request_id(), 'reject', '{} {}'.format(consumer_id, msg_id), confirm=confirm)

### reject all

def reject_all(consumer_id, confirm=False):
    """
    Reject all messages - to return them to the queue with incremented "retry" counter.

    @param consumer_id: str
    @param confirm: bool
    """
    reject(consumer_id, '--all', confirm=confirm)

### delete consumer

def delete_consumer(consumer_id, confirm=False):
    """
    Delete the consumer.
    Server will not send messages to this consumer any more.
    Client will not restart this consumer on reconnect.

    @param consumer_id: str
    @param confirm: bool
    """
    _consumers.pop(consumer_id, None)
    _on_msg.pop(consumer_id, None)
    _on_disconnect.pop(consumer_id, None)
    _send(_request_id(), 'delete_consumer', consumer_id, confirm=confirm)

### delete queue

def delete_queue(queue, confirm=False):
    """
    Delete the queue instantly.
    Server will not copy messages to this queue any more.

    @param queue: str
    @param confirm: bool
    """
    _send(_request_id(), 'delete_queue', queue, confirm=confirm)

### ping

def ping(data=None, confirm=False):
    """
    Ping-pong. Used by MQKS client for keep-alive.

    @param data: str
    @param confirm: bool
    """
    _send(_request_id(), 'ping', data or config.logger_name, confirm=confirm)

### _ping

def _ping():
    while 1:
        try:
            time.sleep(config.ping_seconds)
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

def _send(request_id, action, data, confirm=False, async=True):
    """
    Send request

    @param request_id: str
    @param action: str
    @param data: str
    @param confirm: bool
    @param async: bool
    """

    action_confirm = action + (' --confirm' if confirm else '')
    config._log.debug('#{} > {} {}'.format(request_id, action_confirm, data))
    request = '{} {} {}\n'.format(request_id, action_confirm, data)

    if async:
        _requests.put(request)
    else:
        config._sock.sendall(request)

    if confirm:
        _confirms[request_id].wait()
        _confirms.pop(request_id, None)

### _sender

def _sender():
    """
    Send requests from queue to sock
    to avoid "This socket is already used by another greenlet" error.
    """

    while 1:
        try:
            try:
                request = _requests.peek(timeout=1)
            except Empty:
                continue

            try:
                config._sock.sendall(request)
            except Exception:
                time.sleep(config.reconnect_seconds)  # Give other clients time to reconnect and start consuming.
                raise

            _requests.get()  # Delete request from queue.

        except Exception as e:
            if not is_disconnect(e):
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
                response = response.rstrip('\r\n')  # Not trailing space in "ok " confirm.
                request_id, response_type, data = response.split(' ', 2)
                config._log.debug('#{} < {} {}'.format(request_id, response_type, data))

                if response_type == 'error':
                    raise Exception(response)

                if data == '':  # "ok " confirm.
                    _confirms[request_id].set()
                    continue

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

    except Exception as e:
        if not is_disconnect(e):
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

### is_disconnect

def is_disconnect(e):
    e = repr(e)
    return 'Connection reset by peer' in e or 'Broken pipe' in e

### tests

def _tests():

    ### import

    import gevent.monkey
    gevent.monkey.patch_all()

    import sys

    import critbot.plugins.syslog
    from critbot import crit_defaults
    crit_defaults.plugins = [critbot.plugins.syslog.plugin(logger_name=config.logger_name, logger_level=logging.INFO)]

    ### main

    connect()

    N = 30 * 1000
    state = adict(consumed=0)
    done = Event()

    def publisher():
        for data in xrange(N, 0, -1):
            publish('user_updated', data)  # confirm=True
            # time.sleep(0)

    def on_msg(msg):
        # config._log.info('got {}'.format(msg))
        # msg.ack()

        state.consumed += 1
        if state.consumed == N:
            done.set()

    def on_disconnect():
        config._log.info('on_disconnect!')

    consumer_id = consume('q1', ['user_updated'], on_msg, on_disconnect=on_disconnect, delete_queue_when_unused=True, confirm=True)  # manual_ack=True

    start = time.time()
    spawn(publisher)
    done.wait()
    print(time.time() - start)

    delete_consumer(consumer_id)

if __name__ == '__main__':
    _tests()
