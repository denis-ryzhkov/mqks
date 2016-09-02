#!/usr/bin/env python

"""
Server of "mqks" - Message Queue Kept Simple.

@author Denis Ryzhkov <denisr@denisr.com>
"""

### become cooperative

import gevent.monkey
gevent.monkey.patch_all()

### import

from adict import adict
from collections import defaultdict
import critbot.plugins.syslog
from critbot import crit, crit_defaults
from gevent import spawn
from gevent.queue import Queue, Empty
from gevent.server import StreamServer
import logging
import time
from uqid import uqid

### config

config = adict(
    name='mqks.server',
    host='',  # All interfaces.
    port=54321,
    block_seconds=1,
    logger_level=logging.INFO,
    id_length=24,
)

crit_defaults.plugins = [critbot.plugins.syslog.plugin(logger_name=config.name, logger_level=config.logger_level)]
log = logging.getLogger(config.name)

### state

queues = defaultdict(Queue)
queues_by_events = defaultdict(set)
consumer_ids_by_clients = defaultdict(set)
messages_by_consumer_ids = defaultdict(dict)
queues_by_consumer_ids = dict()
auto_delete_queues = set()

### action

actions = {}

def action(func):
    actions[func.__name__] = func
    return func

### subscribe

@action
def subscribe(request):
    parts = request.data.split(' ')
    queue = parts.pop(0)
    for event in parts:
        queues_by_events[event].add(queue)

### publish

@action
def publish(request):
    event, data = request.data.split(' ', 1)
    msg = '{} {} {}'.format(request.id, event, data)
    for queue in queues_by_events[event]:
        queues[queue].put(msg)

### consume

@action
def consume(request):
    consumer_id = request.id
    parts = request.data.split(' ')
    queue = parts.pop(0)
    manual_ack = '--manual_ack' in parts

    queues_by_consumer_ids[consumer_id] = queue
    queue = queues[queue]

    consumer_ids = consumer_ids_by_clients[request.client]
    consumer_ids.add(consumer_id)

    while consumer_id in consumer_ids:
        try:
            data = queue.get(timeout=config.block_seconds)
        except Empty:
            continue

        if consumer_id in consumer_ids:  # Consumer could be deleted while being blocked above.

            if manual_ack:
                msg_id, _ = data.split(' ', 1)
                messages_by_consumer_ids[consumer_id][msg_id] = data

            response(request, data)
        else:
            queue.put(data)

### ack

@action
def ack(request):
    consumer_id, msg_id = request.data.split(' ', 1)
    if msg_id == '--all':
        messages_by_consumer_ids.pop(consumer_id, {}).clear()
    else:
        messages_by_consumer_ids[consumer_id].pop(msg_id, None)

### reject

@action
def reject(request):
    consumer_id, msg_id = request.data.split(' ', 1)
    _reject(consumer_id, msg_id)

def _reject(consumer_id, msg_id):

    queue = queues_by_consumer_ids.get(consumer_id)
    if queue is None: return
    queue = queues[queue]

    if msg_id == '--all':
        for msg in messages_by_consumer_ids.pop(consumer_id, {}).values():
            queue.put(msg)

    else:
        msg = messages_by_consumer_ids[consumer_id].pop(msg_id, None)
        if msg is not None:
            queue.put(msg)

### delete_consumer

@action
def delete_consumer(request):
    consumer_id = request.data

    consumer_ids_by_clients[request.client].discard(consumer_id)
    _reject(consumer_id, '--all')
    queue = queues_by_consumer_ids.pop(consumer_id, None)

    if queue in auto_delete_queues and queue not in queues_by_consumer_ids.values():
        delete_queue(adict(data=queue))

### delete_consumers

def delete_consumers(client):
    for consumer_id in list(consumer_ids_by_clients.get(client, {})):
        # Not ".pop()" - "delete_consumer" needs to discard "consumer_id" first, then "reject".
        # "list()" is used to avoid "Set changed size during iteration".
        delete_consumer(adict(client=client, data=consumer_id))
    consumer_ids_by_clients.pop(client, {}).clear()

### delete_queue

@action
def delete_queue(request):
    parts = request.data.split(' ')
    queue = parts.pop(0)
    auto = '--auto' in parts

    if auto:
        auto_delete_queues.add(queue)
    else:
        auto_delete_queues.discard(queue)
        for event in queues_by_events:
            queues_by_events[event].discard(queue)
        queues.pop(queue, None)

### on_request

def on_request(request):
    request.id = None
    try:
        request.body = request.body.rstrip()
        request.id, request.action, request.data = request.body.split(' ', 2)
        log.debug('{}#{} > {} {}'.format(request.client, request.id, request.action, request.data))

        action = actions[request.action]
        action(request)

    except Exception:
        request.error_id = uqid(config.id_length)
        crit(also=request)
        try:
            response(request)
        except Exception:
            crit(also=request)

### response

def response(request, data=None):
    error_id = request.get('error_id')
    data = '{} {}'.format('error' if error_id else 'ok', error_id or data)
    log.debug('{}#{} < {}'.format(request.client, request.id, data))
    request.sock.sendall('{} {}\n'.format(request.id, data))

### on_client

def on_client(sock, addr):
    client = None
    try:
        client = '{}:{}'.format(*addr)
        log.info('new client {}'.format(client))

        f = sock.makefile('r')
        while 1:
            request = adict(client=client, sock=sock)
            request.body = f.readline()
            if request.body == '':
                break

            spawn(on_request, request)

    except Exception:
        crit(also=dict(client=client))

    finally:
        log.info('bye client {}'.format(client))
        try:
            delete_consumers(client)
        except Exception:
            crit(also=dict(client=client))

### main

def main():
    try:
        log.info('listening {host}:{port}'.format(**config))
        StreamServer((config.host, config.port), on_client).serve_forever()

    except Exception:
        crit()

if __name__ == '__main__':
    main()

# TODO: Only if CPU becomes bottleneck, use Python "multiprocessing" stdlib to exploit all cores.
