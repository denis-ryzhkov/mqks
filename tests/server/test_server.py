"""
Test MQKS Server
"""

### import

import time

from mqks.server.config import config
from mqks.tests.cases import MqksTestCase

### TestServer

class TestServer(MqksTestCase):

    ### test request

    def test_request(self):
        # correct request
        client = self.get_simple_client('e1')
        request_id = client.send('publish e1 mydata')
        response = client.get_response(request_id, timeout=0.1)
        self.assertTrue(response is None)

        # bad request
        request_id = client.send('blabla 123')
        response = client.get_response(request_id).split(' ', 1)
        self.assertEqual(response[0], 'error', response[0])
        self.assertEqual(len(response[1]), config['id_length'], len(response[1]))
