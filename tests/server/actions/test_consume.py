"""
Test MQKS Server Consume
"""

### import

from mqks.tests.cases import MqksTestCase

### TestConsume

class TestConsume(MqksTestCase):

    ### test simple consume

    def test_simple_consume(self):
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
        self.assertEqual(msg[2], 'event=e1', msg[2])
        self.assertEqual(msg[3], '1', msg[3])
        msg = q1_client.get_response(consumer_id, timeout=0.1)
        self.assertTrue(msg is None)

        # delete

        request_id = q1_client.send('delete_consumer --confirm {}'.format(consumer_id))
        self.assertEqual(q1_client.get_response(request_id), 'ok ')

        request_id = q1_client.send('delete_queue --confirm q1')
        self.assertEqual(q1_client.get_response(request_id), 'ok ')

    ### test add events on consume

    def test_add_events_on_consume(self):
        q1_client = self.get_simple_client('q1')

        # consume
        consumer_id = q1_client.send('consume --confirm q1 e1')
        self.assertEqual(q1_client.get_response(consumer_id), 'ok ')
        # delete consumer, not queue
        request_id = q1_client.send('delete_consumer --confirm {}'.format(consumer_id))
        self.assertEqual(q1_client.get_response(request_id), 'ok ')

        # consume --add
        consumer_id = q1_client.send('consume --confirm q1 --add e2')
        self.assertEqual(q1_client.get_response(consumer_id), 'ok --update q1 e1 e2')
        self.assertEqual(q1_client.get_response(consumer_id), 'ok ')
        # publish/get e1
        e1_client = self.get_simple_client('e1')
        e1_client.send('publish e1 1')
        msg = q1_client.get_response(consumer_id).split(' ', 3)
        self.assertEqual(msg[2], 'event=e1', msg[2])
        # publish/get e2
        e2_client = self.get_simple_client('e2')
        e2_client.send('publish e2 2')
        msg = q1_client.get_response(consumer_id).split(' ', 3)
        self.assertEqual(msg[2], 'event=e2', msg[2])
        # publish/DON'T get e3
        e3_client = self.get_simple_client('e3')
        e3_client.send('publish e3 3')
        msg = q1_client.get_response(consumer_id, timeout=0.1)
        self.assertTrue(msg is None)

        # delete
        request_id = q1_client.send('delete_consumer --confirm {}'.format(consumer_id))
        self.assertEqual(q1_client.get_response(request_id), 'ok ')
        request_id = q1_client.send('delete_queue --confirm q1')
        self.assertEqual(q1_client.get_response(request_id), 'ok ')

    ### test two different consumers

    def test_two_different_consumers(self):
        # consume first
        q1_client = self.get_simple_client('q1')
        first_consumer_id = q1_client.send('consume q1 e1')
        # consume second
        q2_client = self.get_simple_client('q2')
        second_consumer_id = q2_client.send('consume --confirm q2 e1')
        self.assertEqual(q2_client.get_response(second_consumer_id), 'ok ')
        # publish message
        e1_client = self.get_simple_client('e1')
        publish_id = e1_client.send('publish e1 1')

        msg = q1_client.get_response(first_consumer_id).split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])

        msg = q2_client.get_response(second_consumer_id).split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])

        # delete
        request_id = q1_client.send('delete_consumer --confirm {}'.format(first_consumer_id))
        self.assertEqual(q1_client.get_response(request_id), 'ok ')
        request_id = q2_client.send('delete_consumer --confirm {}'.format(second_consumer_id))
        self.assertEqual(q2_client.get_response(request_id), 'ok ')
        request_id = q1_client.send('delete_queue --confirm q1')
        self.assertEqual(q1_client.get_response(request_id), 'ok ')
        request_id = q2_client.send('delete_queue --confirm q2')
        self.assertEqual(q2_client.get_response(request_id), 'ok ')

    ### test two same consumers

    def test_two_same_consumers(self):
        q1_client1 = self.get_simple_client('q1')
        q1_client2 = self.get_simple_client('q1')
        # consume 1
        consumer_id1 = q1_client1.send('consume q1 e1')
        # consume 2
        consumer_id2 = q1_client2.send('consume --confirm q1 e1')
        self.assertEqual(q1_client2.get_response(consumer_id2), 'ok ')
        # publish message
        e1_client = self.get_simple_client('e1')
        publish_id = e1_client.send('publish e1 1')

        msg = q1_client1.get_response(consumer_id1, timeout=0.1)
        q1_client1_got_msg = msg is not None
        if not q1_client1_got_msg:
            msg = q1_client2.get_response(consumer_id2, timeout=0.1)

        msg = msg.split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])
        self.assertEqual(msg[2], 'event=e1', msg[2])
        self.assertEqual(msg[3], '1', msg[3])
        msg = (q1_client2 if q1_client1_got_msg else q1_client1).get_response((consumer_id2 if q1_client1_got_msg else consumer_id1), timeout=0.1)
        self.assertTrue(msg is None)

        # publish message
        e1_client = self.get_simple_client('e1')
        publish_id = e1_client.send('publish e1 2')

        msg = q1_client1.get_response(consumer_id1, timeout=0.1)
        q1_client1_got_msg = msg is not None
        if not q1_client1_got_msg:
            msg = q1_client2.get_response(consumer_id2, timeout=0.1)

        msg = msg.split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])
        self.assertEqual(msg[2], 'event=e1', msg[2])
        self.assertEqual(msg[3], '2', msg[3])
        msg = (q1_client2 if q1_client1_got_msg else q1_client1).get_response((consumer_id2 if q1_client1_got_msg else consumer_id1), timeout=0.1)
        self.assertTrue(msg is None)

        # delete

        request_id = q1_client1.send('delete_consumer --confirm {}'.format(consumer_id1))
        self.assertEqual(q1_client1.get_response(request_id), 'ok ')

        request_id = q1_client2.send('delete_consumer --confirm {}'.format(consumer_id2))
        self.assertEqual(q1_client2.get_response(request_id), 'ok ')

        request_id = q1_client1.send('delete_queue --confirm q1')
        self.assertEqual(q1_client1.get_response(request_id), 'ok ')
