"""
Test MQKS Server Eval
"""

### import

from mqks.tests.cases import MqksTestCase
import multiprocessing as mp

### workers

workers = mp.cpu_count()

### get_worker

def get_worker(item):
    """
    Find out which worker serves this item, e.g. queue name.
    Items are almost uniformly distributed by workers using hash of item.

    @param item: hashable
    @return int
    """
    return hash(item) % workers

### TestEval

class TestEval(MqksTestCase):

    ### test eval

    def test_eval(self):
        # client
        client = self.get_simple_client()

        # consume
        consumer_id = client.send('consume --confirm q1 e1')
        self.assertEqual(client.get_response(consumer_id), 'ok ')

        # publish message
        publish_id = client.send('publish e1 1')

        # worker
        worker = get_worker('q1')

        # eval
        eval_id = client.send('_eval --worker={} len(state.queues)'.format(worker))
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

        # delete

        request_id = client.send('delete_consumer --confirm {}'.format(consumer_id))
        self.assertEqual(client.get_response(request_id), 'ok ')

        request_id = client.send('delete_queue --confirm q1')
        self.assertEqual(client.get_response(request_id), 'ok ')

        # eval
        eval_id = client.send('_eval --worker={} len(state.queues)'.format(worker))
        msg = client.get_response(eval_id).split(' ', 1)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], '0', msg[1])

