"""
Test MQKS Server Eval
"""

### import

from mqks.tests.cases import MqksTestCase

### TestEval

class TestEval(MqksTestCase):

    ### test eval

    def test_eval(self):
        # client
        client = self.get_simple_client()
        # consume
        consumer_id = client.send('consume --confirm q1 e1')
        self.assertEqual(client.get_response(consumer_id).split(' ', 1)[0], 'ok')
        # publish message
        publish_id = client.send('publish e1 1')

        eval_id = client.send('_eval len(state.queues)')
        msg = client.get_response(eval_id).split(' ', 1)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], '1', msg[1])

        # get message
        msg = client.get_response(consumer_id).split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])
        self.assertEqual(msg[2], 'event=e1', msg[2])
        self.assertEqual(msg[3], '1', msg[3])
        msg = client.get_response(consumer_id, timeout=0.1)
        self.assertTrue(msg is None)

        # delete queue
        client.send('delete_queue q1')
        # delete consumer
        client.send('delete_consumer {}'.format(consumer_id))

        eval_id = client.send('_eval len(state.queues)')
        msg = client.get_response(eval_id).split(' ', 1)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], '0', msg[1])

