"""
Test MQKS Server Delete consumer
"""

### import

from mqks.tests.cases import MqksTestCase

### TestDeleteConsumer

class TestDeleteConsumer(MqksTestCase):
    ### test delete consumer

    def test_delete_consumer(self):
        q1_client = self.get_simple_client('q1')
        # consume
        consumer_id = q1_client.send('consume --confirm q1 e1')
        self.assertEqual(q1_client.get_response(consumer_id), 'ok ')
        # publish message
        e1_client = self.get_simple_client('e1')
        publish_id = e1_client.send('publish e1 1')
        # get message
        msg = q1_client.get_response(consumer_id).split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])
        # delete consumer
        request_id = q1_client.send('delete_consumer --confirm {}'.format(consumer_id))
        self.assertEqual(q1_client.get_response(request_id), 'ok ')
        # publish message
        e1_client.send('publish e1 1')
        msg = q1_client.get_response(consumer_id, timeout=0.1)
        self.assertTrue(msg is None)

        request_id = q1_client.send('delete_queue --confirm q1')
        self.assertEqual(q1_client.get_response(request_id), 'ok ')
