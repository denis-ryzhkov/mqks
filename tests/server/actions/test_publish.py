"""
Test MQKS Server Publish
"""

### import

from mqks.tests.cases import MqksTestCase

### TestPublish

class TestPublish(MqksTestCase):

    ### test publish

    def test_publish(self):
        client = self.get_simple_client()
        # consume
        consumer_id = client.send('consume --confirm q1 e1')
        self.assertEqual(client.get_response(consumer_id).split(' ', 1)[0], 'ok')
        # publish messages
        publishes = [client.send('publish e1 {}'.format(x)) for x in range(3)]
        # get messages
        msgs = [client.get_response(consumer_id).split(' ', 3) for x in range(3)]

        for x in range(3):
            msg = msgs[x]
            self.assertEqual(msg[0], 'ok', msg[0])
            self.assertEqual(msg[1], publishes[x], msg[1])
            self.assertEqual(msg[2], 'event=e1', msg[2])
            self.assertEqual(msg[3], str(x), msg[3])

        msg = client.get_response(consumer_id, timeout=0.1)
        self.assertTrue(msg is None)

        # delete queue
        client.send('delete_queue q1')
        # delete consumer
        client.send('delete_consumer {}'.format(consumer_id))
