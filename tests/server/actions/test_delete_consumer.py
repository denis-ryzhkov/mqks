"""
Test MQKS Server Delete consumer
"""

### import

from mqks.tests.cases import MqksTestCase

### TestDeleteConsumer

class TestDeleteConsumer(MqksTestCase):
    ### test delete consumer

    def test_delete_consumer(self):
        client = self.get_simple_client()
        # consume
        consumer_id = client.send('consume --confirm q1 e1')
        self.assertEqual(client.get_response(consumer_id).split(' ', 1)[0], 'ok')
        # publish message
        publish_id = client.send('publish e1 1')
        # get message
        msg = client.get_response(consumer_id).split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])
        # delete consumer
        client.send('delete_consumer {}'.format(consumer_id))
        # publish message
        client.send('publish e1 1')
        msg = client.get_response(consumer_id, timeout=0.1)
        self.assertTrue(msg is None)

        client.send('delete_queue q1')
