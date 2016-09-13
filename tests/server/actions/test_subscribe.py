"""
Test MQKS Server Subscribe
"""

### import

from mqks.tests.cases import MqksTestCase

### TestSubscribe

class TestSubscribe(MqksTestCase):

    ### test subscribe

    def test_subscribe(self):
        client = self.get_simple_client()
        # consume
        consumer_id = client.send('consume q1')
        # publish message
        publish_id = client.send('publish e1 1')
        # try to consume
        msg = client.get_response(consumer_id, timeout=0.1)
        self.assertTrue(msg is None)

        # subscribe
        client.send('subscribe q1 e1')
        # publish message
        publish_id = client.send('publish e1 2')
        # get message
        msg = client.get_response(consumer_id).split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])
        self.assertEqual(msg[2], 'event=e1', msg[2])
        self.assertEqual(msg[3], '2', msg[3])

        # re-subscribe
        client.send('subscribe q1 e2')
        # publish bad message
        publish_id = client.send('publish e1 3')
        # try to consume
        msg = client.get_response(consumer_id, timeout=0.1)
        self.assertTrue(msg is None)
        # publish good message
        publish_id = client.send('publish e2 4')
        # get message
        msg = client.get_response(consumer_id).split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])
        self.assertEqual(msg[2], 'event=e2', msg[2])
        self.assertEqual(msg[3], '4', msg[3])


        # delete queue
        client.send('delete_queue q1')
        # delete consumer
        client.send('delete_consumer {}'.format(consumer_id))
