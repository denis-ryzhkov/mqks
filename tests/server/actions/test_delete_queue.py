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
        # consume
        consumer_id = client.send('consume --confirm q1 e1')
        self.assertEqual(client.get_response(consumer_id), 'ok ')
        # publish message
        publish_id = client.send('publish e1 1')
        # get message
        msg = client.get_response(consumer_id).split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])
        self.assertEqual(msg[3], '1', msg[3])
        # delete consumer
        request_id = client.send('delete_consumer --confirm {}'.format(consumer_id))
        self.assertEqual(client.get_response(request_id), 'ok ')

        # publish message
        publish_id = client.send('publish e1 2')
        msg = client.get_response(consumer_id, timeout=0.1)
        self.assertTrue(msg is None)

        # consume
        consumer_id = client.send('consume --confirm q1 e1')
        self.assertEqual(client.get_response(consumer_id), 'ok ')
        msg = client.get_response(consumer_id).split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])
        self.assertEqual(msg[3], '2', msg[3])

        # delete queue
        request_id = client.send('delete_queue --confirm q1')
        self.assertEqual(client.get_response(request_id), 'ok ')

        client.send('publish e1 2')
        msg = client.get_response(consumer_id, timeout=0.1)
        self.assertTrue(msg is None)

        # delete consumer
        client.send('delete_consumer {}'.format(consumer_id))
        # Don't wait --confirm here: queue is deleted, routing to consumer is lost.

    ### test delete queue when unused

    def test_delete_queue_when_unused(self):
        client = self.get_simple_client()
        # consume
        consumer_id = client.send('consume --confirm q1 e1 --delete-queue-when-unused')
        self.assertEqual(client.get_response(consumer_id), 'ok ')
        # publish message
        publish_id = client.send('publish e1 1')
        # get message
        msg = client.get_response(consumer_id).split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])
        self.assertEqual(msg[3], '1', msg[3])
        # delete consumer
        request_id = client.send('delete_consumer --confirm {}'.format(consumer_id))
        self.assertEqual(client.get_response(request_id), 'ok ')

        # publish message
        client.send('publish e1 2')
        msg = client.get_response(consumer_id, timeout=0.1)
        self.assertTrue(msg is None)

        # consume
        consumer_id = client.send('consume q1 e1 --delete-queue-when-unused')
        msg = client.get_response(consumer_id, timeout=0.1)
        self.assertTrue(msg is None)
        # delete consumer
        request_id = client.send('delete_consumer --confirm {}'.format(consumer_id))
        self.assertEqual(client.get_response(request_id), 'ok ')

    ### test delete queue when unused timeout

    def test_delete_queue_when_unused_timeout(self):
        client = self.get_simple_client()
        # consume
        consumer_id = client.send('consume --confirm q1 e1 --delete-queue-when-unused=1')
        self.assertEqual(client.get_response(consumer_id), 'ok ')
        # publish message
        publish_id = client.send('publish e1 1')
        # get message
        msg = client.get_response(consumer_id).split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])
        self.assertEqual(msg[3], '1', msg[3])
        # delete consumer
        request_id = client.send('delete_consumer --confirm {}'.format(consumer_id))
        self.assertEqual(client.get_response(request_id), 'ok ')

        # publish message
        publish_id = client.send('publish e1 2')
        msg = client.get_response(consumer_id, timeout=0.1)
        self.assertTrue(msg is None)

        # consume
        consumer_id = client.send('consume --confirm q1 e1 --delete-queue-when-unused=1')
        self.assertEqual(client.get_response(consumer_id), 'ok ')
        msg = client.get_response(consumer_id).split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])
        self.assertEqual(msg[3], '2', msg[3])
        # delete consumer
        request_id = client.send('delete_consumer --confirm {}'.format(consumer_id))
        self.assertEqual(client.get_response(request_id), 'ok ')

        # wait more than --delete-queue-when-unused=1
        time.sleep(1.1)

        # publish message
        client.send('publish e1 3')

        # consume
        consumer_id = client.send('consume --confirm q1 e1 --delete-queue-when-unused=1')
        self.assertEqual(client.get_response(consumer_id), 'ok ')
        msg = client.get_response(consumer_id, timeout=0.1)
        self.assertTrue(msg is None, msg)

        # delete

        request_id = client.send('delete_consumer --confirm {}'.format(consumer_id))
        self.assertEqual(client.get_response(request_id), 'ok ')

        request_id = client.send('delete_queue --confirm q1')
        self.assertEqual(client.get_response(request_id), 'ok ')
