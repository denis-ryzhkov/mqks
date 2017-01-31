#!/usr/bin/env python

"""
Random load "mqks server"

Consume random qeues with random option and random events
Publish random messages to random events
Random reconnects

Usage:
    ./tests/random_load.py

You can run a few random_load.py at the same time
"""

### import

if __name__ == '__main__':

    import gevent.monkey
    gevent.monkey.patch_all()

    import os, sys
    sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))

import sys
import random
import time
from adict import adict
from uqid import uqid
from gevent import spawn
from mqks.client import mqks
from mqks.server.config import config as server_config

### config

config = adict(
    consumers_max=1000,
    consumer_ttl_seconds=(60, 600),  # None
    consumer_msg_works_seconds=(0.01, 0.3),
    publishers=(60, 60),
    publish_wait_seconds=(0.006, 0.006),
    publish_multiplicator=32,
    data_len_max=1024 * 2,
    reconnect_ttl_seconds=(3600, 86700),  # None
)

### state

state = adict(
    consumers=dict(),
    published_num=0,
    consumed_num=0,
    client_expired_time=None if config.reconnect_ttl_seconds is None else (time.time() + random.randint(*config.reconnect_ttl_seconds))
)

### _random consume

def _random_consume():
    """
    Consume random queue
    @return:
    """
    qn = random.randint(0, config.consumers_max)
    queue = "q{}".format(qn)
    events = ["e{}".format(qn % (config.consumers_max / config.publish_multiplicator))]
    delete_queue_when_unused = True
    if random.randint(0, 100) > 20:
        delete_queue_when_unused = True
        if random.randint(0, 100) > 80:
            delete_queue_when_unused = random.randint(1, 5)

    consumer_id = mqks.consume(queue, events, _on_msg, delete_queue_when_unused=delete_queue_when_unused)
    state.consumers[consumer_id] = adict(
        delete_queue_when_unused=delete_queue_when_unused,
        consumer_expired_time=None if config.consumer_ttl_seconds is None else (time.time() + random.randint(*config.consumer_ttl_seconds))
    )

### on msg

def _on_msg(msg):
    state.consumed_num += 1
    time.sleep(random.uniform(*config.consumer_msg_works_seconds))

### consumer creator

def consumer_creator():
    while 1:
        if len(state.consumers) < config.consumers_max:
            _random_consume()
        time.sleep(0.01)

### consumer killer

def consumer_killer():
    while 1:
        try:
            for consumer_id, params in state.consumers.iteritems():
                if params.consumer_expired_time is None or time.time() < params.consumer_expired_time:
                    continue

                mqks.delete_consumer(consumer_id)
                del state.consumers[consumer_id]
        except RuntimeError:
            pass

        time.sleep(1)

### publisher

def publisher():
    while 1:
        mqks.publish(
            event="e{}".format(random.randint(0, (config.consumers_max / config.publish_multiplicator) - 1)),
            data='{{"myid": "{}"}}'.format(uqid(random.randint(1, config.data_len_max)))
        )
        state.published_num += 1
        time.sleep(random.uniform(*config.publish_wait_seconds))

### reconnector

def reconnector():
    while 1:
        if config.reconnect_ttl_seconds is not None and state.client_expired_time <= time.time():
            mqks.disconnect()
            # clear consumers without mqks.delete_consumer
            state.consumers = dict()

            time.sleep(1)

            mqks.connect()
            state.client_expired_time = time.time() + random.randint(*config.reconnect_ttl_seconds)

        time.sleep(1)

### print stats

def print_stats():
    while 1:
        sys.stdout.write("published: {}, consumed: {}, consumers: {}\r".format(state.published_num, state.consumed_num, len(state.consumers)))
        sys.stdout.flush()
        time.sleep(1)

### main

def main():

    import critbot.plugins.syslog
    from critbot import crit_defaults
    import logging
    crit_defaults.plugins = [critbot.plugins.syslog.plugin(logger_name=mqks.config.logger_name, logger_level=logging.INFO)]

    mqks.config.update(
        host=server_config.host,
        port=server_config.port,
    )

    mqks.connect()

    spawn(consumer_creator)
    spawn(consumer_killer)
    spawn(publisher)
    spawn(reconnector)
    spawn(print_stats)

    while 1:
        time.sleep(1)

### __main__

if __name__ == '__main__':
    main()
