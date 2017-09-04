"""
Test MQKS Server Eval
"""

### import

from mqks.tests.cases import get_worker, MqksTestCase

### TestEval

class TestEval(MqksTestCase):

    ### test eval

    def test_eval(self):
        # client
        q1_client = self.get_simple_client('q1')

        # consume
        consumer_id = q1_client.send('consume --confirm q1 e1')
        self.assertEqual(q1_client.get_response(consumer_id), 'ok ')

        # publish message
        e1_client = self.get_simple_client('e1')
        publish_id = e1_client.send('publish e1 1')

        # worker
        worker = get_worker('q1')

        # eval
        eval_id = q1_client.send('_eval --worker={} len(state.queues)'.format(worker))
        msg = q1_client.get_response(eval_id).split(' ', 1)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], '1', msg[1])

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

        # eval
        eval_id = q1_client.send('_eval --worker={} len(state.queues)'.format(worker))
        msg = q1_client.get_response(eval_id).split(' ', 1)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], '0', msg[1])

