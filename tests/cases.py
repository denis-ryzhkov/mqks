
### import

import gevent
import unittest

from mqks.server.config import config
from mqks.tests.simple_client import SimpleClient

### get_worker

WORKERS = len(config['workers'])

def get_worker(item):
    """
    Find out which worker serves this item, e.g. queue name.
    Items are almost uniformly distributed by workers using hash of item.

    @param item: hashable
    @return int
    """
    return hash(item) % WORKERS

### MqksTestCase

class MqksTestCase(unittest.TestCase):

    ### get simple client

    def get_simple_client(self, item):
        """
        Get simple client, connected to worker of this item.
        Used to test server.

        @param item: hashable
        @return: SimpleClient
        """
        worker = get_worker(item)
        host, _, port = config['workers'][worker].split(':')
        port = int(port)
        client = SimpleClient(host, port)
        client.wait_server()
        client.connect()
        return client

### prepare_main_client

def prepare_main_client():
    """
    Configure and connect main "mqks.client".
    """
    from critbot import crit_defaults
    import critbot.plugins.syslog
    from mqks.client import mqks
    from mqks.server.config import config as server_config

    crit_defaults.plugins = [critbot.plugins.syslog.plugin(logger_name=mqks.config['logger_name'], logger_level=server_config['logger_level'])]
    mqks.config['workers'] = server_config['workers']
    mqks.connect()

### Mock

class Mock(object):
    """
    Simple mock object to test callbacks like "on_msg", etc.
    """

    def __init__(self):
        self.called = 0
        self.args = None
        self.kwargs = None

    def __call__(self, *args, **kwargs):
        self.called += 1
        self.args = args
        self.kwargs = kwargs
