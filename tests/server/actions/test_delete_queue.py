"""
Test MQKS Server Delete queue
"""

### import

import time
from mqks.tests.cases import MqksTestCase

### TestDeleteQueue

class TestDeleteQueue(MqksTestCase):

    ### test delete queue

    def test_delete_queue(self):
        client = self.get_simple_client()
        # subscribe
        client.send('subscribe q1 e1')
        # consume
        consumer_id = client.send('consume q1')
        # publish message
        publish_id = client.send('publish e1 1')
        # get message
        msg = client.get_response(consumer_id).split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])
        self.assertEqual(msg[3], '1', msg[3])
        # delete consumer
        client.send('delete_consumer {}'.format(consumer_id))

        # publish message
        publish_id = client.send('publish e1 2')
        msg = client.get_response(consumer_id, timeout=0.1)
        self.assertTrue(msg is None)

        # consume
        consumer_id = client.send('consume q1')
        msg = client.get_response(consumer_id).split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])
        self.assertEqual(msg[3], '2', msg[3])

        # delete queue
        client.send('delete_queue q1')
        time.sleep(0.1)

        client.send('publish e1 2')
        msg = client.get_response(consumer_id, timeout=0.1)
        self.assertTrue(msg is None)
        # delete consumer
        client.send('delete_consumer {}'.format(consumer_id))

    ### test delete queue when unused

    def test_delete_queue_when_unused(self):
        client = self.get_simple_client()
        # subscribe
        client.send('subscribe q1 e1')
        # consume
        consumer_id = client.send('consume q1')
        # delete queue
        client.send('delete_queue q1 --when-unused')
        # publish message
        publish_id = client.send('publish e1 1')
        # get message
        msg = client.get_response(consumer_id).split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])
        self.assertEqual(msg[3], '1', msg[3])
        # delete consumer
        client.send('delete_consumer {}'.format(consumer_id))
        time.sleep(0.1)

        # publish message
        client.send('publish e1 2')
        msg = client.get_response(consumer_id, timeout=0.1)
        self.assertTrue(msg is None)

        # consume
        consumer_id = client.send('consume q1')
        msg = client.get_response(consumer_id, timeout=0.1)
        self.assertTrue(msg is None)
        # delete consumer
        client.send('delete_consumer {}'.format(consumer_id))

    ### test delete queue when unused timeout

    def test_delete_queue_when_unused_timeout(self):
        client = self.get_simple_client()
        # subscribe
        client.send('subscribe q1 e1')
        # consume
        consumer_id = client.send('consume q1')
        # delete queue
        client.send('delete_queue q1 --when-unused=1')
        # publish message
        publish_id = client.send('publish e1 1')
        # get message
        msg = client.get_response(consumer_id).split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])
        self.assertEqual(msg[3], '1', msg[3])
        # delete consumer
        client.send('delete_consumer {}'.format(consumer_id))
        time.sleep(0.5)

        # publish message
        publish_id = client.send('publish e1 2')
        msg = client.get_response(consumer_id, timeout=0.1)
        self.assertTrue(msg is None)

        # consume
        consumer_id = client.send('consume q1')
        msg = client.get_response(consumer_id).split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])
        self.assertEqual(msg[3], '2', msg[3])
        # delete consumer
        client.send('delete_consumer {}'.format(consumer_id))

        time.sleep(1.1)

        # publish message
        client.send('publish e1 3')

        # consume
        consumer_id = client.send('consume q1')
        msg = client.get_response(consumer_id, timeout=0.1)
        self.assertTrue(msg is None)
        # delete consumer
        client.send('delete_consumer {}'.format(consumer_id))
