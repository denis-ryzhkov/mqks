
### import

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
