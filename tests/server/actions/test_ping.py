"""
Test MQKS Server Ping
"""

### import

from mqks.tests.cases import MqksTestCase

### TestPing

class TestPing(MqksTestCase):

    ### test ping

    def test_ping(self):
        client = self.get_simple_client('any')
        ping_id = client.send('ping d1')
        msg = client.get_response(ping_id)
        msg = msg.split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], 'd1', msg[1])
