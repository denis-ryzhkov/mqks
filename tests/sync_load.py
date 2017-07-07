#!/usr/bin/env python

"""
Load test targeting a lot of "sync_waiting" to grow "commands_waiting".
UPDATE: "sync_waiting" was deleted, test is kept to ensure "commands_waiting" are not growing now.
"""

### import

import gevent.monkey
gevent.monkey.patch_all()

import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))

import critbot.plugins.syslog
from critbot import crit, crit_defaults
from gevent import spawn
import logging
from mqks.client import mqks
from mqks.server.config import config as server_config
import random
import time
from uqid import dtid

### config

crit_defaults.plugins = [critbot.plugins.syslog.plugin(logger_name=mqks.config['logger_name'], logger_level=logging.INFO)]

### test

def test():
    mqks.config.update(
        host=server_config['host'],
        port=server_config['port'],
    )
    mqks.connect()

    for _ in xrange(60*100):
        spawn(flow)
        time.sleep(0.01)

def flow():
    try:
        queue = dtid()
        consumer_id = mqks.consume(queue=queue, events=[queue], on_msg=lambda msg: None, delete_queue_when_unused=1, confirm=True)
        for _ in xrange(10):
            mqks.publish(queue, dtid())
        time.sleep(1)
        mqks.delete_consumer(consumer_id)

    except Exception:
        crit()

if __name__ == '__main__':
    test()
