"""
Test MQKS Server Publish
"""

### import

from mqks.tests.cases import MqksTestCase

### TestPublish

class TestPublish(MqksTestCase):

    ### test publish

    def test_publish(self):
        q1_client = self.get_simple_client('q1')
        # consume
        consumer_id = q1_client.send('consume --confirm q1 e1')
        self.assertEqual(q1_client.get_response(consumer_id), 'ok ')
        # publish messages
        e1_client = self.get_simple_client('e1')
        publishes = [e1_client.send('publish e1 {}'.format(x)) for x in range(3)]
        # get messages
        msgs = [q1_client.get_response(consumer_id).split(' ', 3) for x in range(3)]

        for x in range(3):
            msg = msgs[x]
            self.assertEqual(msg[0], 'ok', msg[0])
            self.assertEqual(msg[1], publishes[x], msg[1])
            self.assertEqual(msg[2], 'event=e1', msg[2])
            self.assertEqual(msg[3], str(x), msg[3])

        msg = q1_client.get_response(consumer_id, timeout=0.1)
        self.assertTrue(msg is None)

        # delete

        request_id = q1_client.send('delete_consumer --confirm {}'.format(consumer_id))
        self.assertEqual(q1_client.get_response(request_id), 'ok ')

        request_id = q1_client.send('delete_queue --confirm q1')
        self.assertEqual(q1_client.get_response(request_id), 'ok ')
