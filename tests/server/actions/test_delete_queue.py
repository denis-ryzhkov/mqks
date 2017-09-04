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
        self.assertEqual(msg[3], '1', msg[3])
        # delete consumer
        request_id = q1_client.send('delete_consumer --confirm {}'.format(consumer_id))
        self.assertEqual(q1_client.get_response(request_id), 'ok ')

        # publish message
        e1_client = self.get_simple_client('e1')
        publish_id = e1_client.send('publish e1 2')
        msg = q1_client.get_response(consumer_id, timeout=0.1)
        self.assertTrue(msg is None)

        # consume
        consumer_id = q1_client.send('consume --confirm q1 e1')
        self.assertEqual(q1_client.get_response(consumer_id), 'ok ')
        msg = q1_client.get_response(consumer_id).split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])
        self.assertEqual(msg[3], '2', msg[3])

        # delete queue
        request_id = q1_client.send('delete_queue --confirm q1')
        self.assertEqual(q1_client.get_response(consumer_id), 'ok --update q1')
        self.assertEqual(q1_client.get_response(request_id), 'ok ')

        e1_client = self.get_simple_client('e1')
        e1_client.send('publish e1 2')
        msg = q1_client.get_response(consumer_id, timeout=0.1)
        self.assertTrue(msg is None)

        # delete consumer
        q1_client.send('delete_consumer {}'.format(consumer_id))
        # Don't wait --confirm here: queue is deleted, routing to consumer is lost.

    ### test delete queue when unused

    def test_delete_queue_when_unused(self):
        q1_client = self.get_simple_client('q1')
        # consume
        consumer_id = q1_client.send('consume --confirm q1 e1 --delete-queue-when-unused')
        self.assertEqual(q1_client.get_response(consumer_id), 'ok ')
        # publish message
        e1_client = self.get_simple_client('e1')
        publish_id = e1_client.send('publish e1 1')
        # get message
        msg = q1_client.get_response(consumer_id).split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])
        self.assertEqual(msg[3], '1', msg[3])
        # delete consumer
        request_id = q1_client.send('delete_consumer --confirm {}'.format(consumer_id))
        self.assertEqual(q1_client.get_response(request_id), 'ok ')

        # publish message
        e1_client.send('publish e1 2')
        msg = e1_client.get_response(consumer_id, timeout=0.1)
        self.assertTrue(msg is None)

        # consume
        consumer_id = q1_client.send('consume q1 e1 --delete-queue-when-unused')
        msg = q1_client.get_response(consumer_id, timeout=0.1)
        self.assertTrue(msg is None)
        # delete consumer
        request_id = q1_client.send('delete_consumer --confirm {}'.format(consumer_id))
        self.assertEqual(q1_client.get_response(request_id), 'ok ')

    ### test delete queue when unused timeout

    def test_delete_queue_when_unused_timeout(self):
        q1_client = self.get_simple_client('q1')
        # consume
        consumer_id = q1_client.send('consume --confirm q1 e1 --delete-queue-when-unused=1')
        self.assertEqual(q1_client.get_response(consumer_id), 'ok ')
        # publish message
        e1_client = self.get_simple_client('e1')
        publish_id = e1_client.send('publish e1 1')
        # get message
        msg = q1_client.get_response(consumer_id).split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])
        self.assertEqual(msg[3], '1', msg[3])
        # delete consumer
        request_id = q1_client.send('delete_consumer --confirm {}'.format(consumer_id))
        self.assertEqual(q1_client.get_response(request_id), 'ok ')

        # publish message
        publish_id = e1_client.send('publish e1 2')
        msg = q1_client.get_response(consumer_id, timeout=0.1)
        self.assertTrue(msg is None)

        # consume
        consumer_id = q1_client.send('consume --confirm q1 e1 --delete-queue-when-unused=1')
        self.assertEqual(q1_client.get_response(consumer_id), 'ok ')
        msg = q1_client.get_response(consumer_id).split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])
        self.assertEqual(msg[3], '2', msg[3])
        # delete consumer
        request_id = q1_client.send('delete_consumer --confirm {}'.format(consumer_id))
        self.assertEqual(q1_client.get_response(request_id), 'ok ')

        # wait more than --delete-queue-when-unused=1
        time.sleep(1.1)

        # publish message
        e1_client.send('publish e1 3')

        # consume
        consumer_id = q1_client.send('consume --confirm q1 e1 --delete-queue-when-unused=1')
        self.assertEqual(q1_client.get_response(consumer_id), 'ok ')
        msg = q1_client.get_response(consumer_id, timeout=0.1)
        self.assertTrue(msg is None, msg)

        # delete

        request_id = q1_client.send('delete_consumer --confirm {}'.format(consumer_id))
        self.assertEqual(q1_client.get_response(request_id), 'ok ')

        request_id = q1_client.send('delete_queue --confirm q1')
        self.assertEqual(q1_client.get_response(request_id), 'ok ')
