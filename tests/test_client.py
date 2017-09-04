#!/usr/bin/env python

"""
Almost manual test of "mqks.client".

TODO: Move to unified test framework with asserts, etc.
"""

import gevent.monkey
gevent.monkey.patch_all()

from gevent import sleep
def g():
    sleep(1)

import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))

from mqks.client import mqks
from mqks.server.config import config as server_config
mqks.config['workers'] = server_config['workers']

from critbot import crit_defaults
import critbot.plugins.syslog
import logging
crit_defaults.plugins = [critbot.plugins.syslog.plugin(logger_name=mqks.config['logger_name'], logger_level=logging.DEBUG)]

mqks.connect()

def on_msg(msg):
     print('on_msg', msg)

def on_reconnect(old_consumer_id, new_consumer_id):
    global consumer_id
    consumer_id = new_consumer_id
    print(on_reconnect, old_consumer_id, new_consumer_id)

consumer_id = mqks.consume('q1', ['e1', 'e2'], on_msg, on_reconnect=on_reconnect, manual_ack=True)
g()

mqks.rebind('q1', ['e1', 'e3'])
g()

print('Check consumer is updated to e1 e3:')
print(mqks.state)

mqks.publish('e1', 'd1')
g()

print(mqks.state)

test_reconnect = True
if test_reconnect:
    old_sock = mqks.state['socks'][1]
    while mqks.state['socks'][1] is old_sock:
        print('restart mqksd')
        g()

    print(mqks.state)

    # No "retry" of because mqksd was restarted.
    g()
    mqks.publish('e1', 'd1')
    g()

mqks.ack_all(consumer_id, confirm=True)
g()
mqks.delete_consumer(consumer_id)
g()
print(mqks.state)

mqks.disconnect()
g()
print(mqks.state)

