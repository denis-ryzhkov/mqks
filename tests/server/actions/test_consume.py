"""
Test MQKS Server Consume
"""

### import

from mqks.tests.cases import MqksTestCase

### TestConsume

class TestConsume(MqksTestCase):

    ### test simple consume

    def test_simple_consume(self):
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
        self.assertEqual(msg[2], 'event=e1', msg[2])
        self.assertEqual(msg[3], '1', msg[3])
        msg = client.get_response(consumer_id, timeout=0.1)
        self.assertTrue(msg is None)

        # delete

        request_id = client.send('delete_consumer --confirm {}'.format(consumer_id))
        self.assertEqual(client.get_response(request_id), 'ok ')

        request_id = client.send('delete_queue --confirm q1')
        self.assertEqual(client.get_response(request_id), 'ok ')

    ### test add events on consume

    def test_add_events_on_consume(self):
        client = self.get_simple_client()

        # consume
        consumer_id = client.send('consume --confirm q1 e1')
        self.assertEqual(client.get_response(consumer_id), 'ok ')
        # delete consumer, not queue
        request_id = client.send('delete_consumer --confirm {}'.format(consumer_id))
        self.assertEqual(client.get_response(request_id), 'ok ')

        # consume --add
        consumer_id = client.send('consume --confirm q1 --add e2')
        self.assertEqual(client.get_response(consumer_id), 'ok ')
        # publish/get e1
        client.send('publish e1 1')
        msg = client.get_response(consumer_id).split(' ', 3)
        self.assertEqual(msg[2], 'event=e1', msg[2])
        # publish/get e2
        client.send('publish e2 2')
        msg = client.get_response(consumer_id).split(' ', 3)
        self.assertEqual(msg[2], 'event=e2', msg[2])
        # publish/DON'T get e3
        client.send('publish e3 3')
        msg = client.get_response(consumer_id, timeout=0.1)
        self.assertTrue(msg is None)

        # delete
        request_id = client.send('delete_consumer --confirm {}'.format(consumer_id))
        self.assertEqual(client.get_response(request_id), 'ok ')
        request_id = client.send('delete_queue --confirm q1')
        self.assertEqual(client.get_response(request_id), 'ok ')

    ### test existing events on consume

    def test_existing_events_on_consume(self):
        client = self.get_simple_client()

        # consume
        consumer_id = client.send('consume --confirm q1 e1')
        self.assertEqual(client.get_response(consumer_id), 'ok ')
        # delete consumer, not queue
        request_id = client.send('delete_consumer --confirm {}'.format(consumer_id))
        self.assertEqual(client.get_response(request_id), 'ok ')

        # consume existing events
        consumer_id = client.send('consume --confirm q1')
        self.assertEqual(client.get_response(consumer_id), 'ok ')
        # publish/get e1
        client.send('publish e1 1')
        msg = client.get_response(consumer_id).split(' ', 3)
        self.assertEqual(msg[2], 'event=e1', msg[2])

        # delete
        request_id = client.send('delete_consumer --confirm {}'.format(consumer_id))
        self.assertEqual(client.get_response(request_id), 'ok ')
        request_id = client.send('delete_queue --confirm q1')
        self.assertEqual(client.get_response(request_id), 'ok ')

    ### test two different consumers

    def test_two_different_consumers(self):
        client = self.get_simple_client()
        # consume first
        first_consumer_id = client.send('consume q1 e1')
        # consume second
        second_consumer_id = client.send('consume --confirm q2 e1')
        self.assertEqual(client.get_response(second_consumer_id), 'ok ')
        # publish message
        publish_id = client.send('publish e1 1')

        msg = client.get_response(first_consumer_id).split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])

        msg = client.get_response(second_consumer_id).split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])

        # delete
        request_id = client.send('delete_consumer --confirm {}'.format(first_consumer_id))
        self.assertEqual(client.get_response(request_id), 'ok ')
        request_id = client.send('delete_consumer --confirm {}'.format(second_consumer_id))
        self.assertEqual(client.get_response(request_id), 'ok ')
        request_id = client.send('delete_queue --confirm q1')
        self.assertEqual(client.get_response(request_id), 'ok ')
        request_id = client.send('delete_queue --confirm q2')
        self.assertEqual(client.get_response(request_id), 'ok ')

    ### test two same consumers

    def test_two_same_consumers(self):
        client1 = self.get_simple_client()
        client2 = self.get_simple_client()
        # consume 1
        consumer_id1 = client1.send('consume q1 e1')
        # consume 2
        consumer_id2 = client2.send('consume --confirm q1 e1')
        self.assertEqual(client2.get_response(consumer_id2), 'ok ')
        # publish message
        publish_id = client1.send('publish e1 1')

        msg = client1.get_response(consumer_id1, timeout=0.1)
        client1_got_msg = msg is not None
        if not client1_got_msg:
            msg = client2.get_response(consumer_id2, timeout=0.1)

        msg = msg.split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])
        self.assertEqual(msg[2], 'event=e1', msg[2])
        self.assertEqual(msg[3], '1', msg[3])
        msg = (client2 if client1_got_msg else client1).get_response((consumer_id2 if client1_got_msg else consumer_id1), timeout=0.1)
        self.assertTrue(msg is None)

        # publish message
        publish_id = client1.send('publish e1 2')

        msg = client1.get_response(consumer_id1, timeout=0.1)
        client1_got_msg = msg is not None
        if not client1_got_msg:
            msg = client2.get_response(consumer_id2, timeout=0.1)

        msg = msg.split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])
        self.assertEqual(msg[2], 'event=e1', msg[2])
        self.assertEqual(msg[3], '2', msg[3])
        msg = (client2 if client1_got_msg else client1).get_response((consumer_id2 if client1_got_msg else consumer_id1), timeout=0.1)
        self.assertTrue(msg is None)

        # delete

        request_id = client1.send('delete_consumer --confirm {}'.format(consumer_id1))
        self.assertEqual(client1.get_response(request_id), 'ok ')

        request_id = client2.send('delete_consumer --confirm {}'.format(consumer_id2))
        self.assertEqual(client2.get_response(request_id), 'ok ')

        request_id = client1.send('delete_queue --confirm q1')
        self.assertEqual(client1.get_response(request_id), 'ok ')
