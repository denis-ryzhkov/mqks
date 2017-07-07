
### import

import unittest

from mqks.server.config import config
from mqks.tests.simple_client import SimpleClient

### MqksTestCase

class MqksTestCase(unittest.TestCase):

    ### get simple client

    def get_simple_client(self, host=None, port=None, auto_connect=True):
        """
        Get simple client
        @param host: str
        @param port: int
        @param auto_connect: bool
        @return: SimpleClient
        """
        client = SimpleClient(
            host=host or config['host'],
            port=port or config['port']
        )
        if auto_connect:
            client.wait_server()
            client.connect()

        return client
