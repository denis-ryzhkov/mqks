"""
Test MQKS client: update consumer, reconnect
"""

### import

import gevent

from mqks.client import mqks
from mqks.tests.cases import Mock, MqksTestCase

### test

class TestUpdateConsumer(MqksTestCase):

    def test_update_consumer(self):

        # Consume:

        on_msg = Mock()
        on_disconnect = Mock()
        on_reconnect = Mock()
        consumer_id = mqks.consume('q1', ['e1', 'e2'], on_msg, on_disconnect=on_disconnect, on_reconnect=on_reconnect, delete_queue_when_unused=5.0, manual_ack=True, confirm=True)

        worker = mqks.get_worker('q1')
        self.assertEqual(mqks.state['consumers'][worker][consumer_id], 'q1 e1 e2 --delete-queue-when-unused=5.0 --manual-ack')

        # Rebind leads to "update consumer" flow:

        mqks.rebind('q1', ['e2', 'e3'], confirm=True)
        self.assertEqual(mqks.state['consumers'][worker][consumer_id], 'q1 e2 e3 --delete-queue-when-unused=5.0 --manual-ack')
        self.assertEqual(mqks._eval("' '.join(sorted(state.events_by_queues['q1']))", worker=worker), 'e2 e3')

        # Reconnect:

        old_client = mqks._eval("request['client']", worker=worker)

        try:
            mqks._eval("state.socks_by_clients[request['client']].shutdown(2)", worker=worker, timeout=1)  # Disconnect.
        except gevent.Timeout:
            pass

        gevent.sleep(2)  # Reconnect.
        self.assertEqual(on_disconnect.called, 1)
        self.assertEqual(on_reconnect.called, 1)

        # consumer_id is updated:

        self.assertEqual(on_reconnect.args[0], consumer_id)
        self.assertNotEqual(on_reconnect.args[1], consumer_id)
        consumer_id = on_reconnect.args[1]

        # client id is updated:

        new_client = mqks._eval("request['client']", worker=worker)
        self.assertNotEqual(old_client, new_client)

        # Reconsume used last consumer config:

        self.assertEqual(mqks._eval("' '.join(sorted(state.events_by_queues['q1']))", worker=worker), 'e2 e3')

        # Message delivery is still working after reconnect:

        self.assertEqual(on_msg.called, 0)
        mqks.publish('e3', 'd1', confirm=True)
        if not on_msg.called:
            gevent.sleep(0.1)
        self.assertEqual(on_msg.called, 1)
        self.assertEqual(on_msg.args[0]['data'], 'd1')

        # Add events:

        mqks.delete_consumer(consumer_id, confirm=True)
        consumer_id = mqks.consume('q1', ['e3', 'e4'], on_msg, delete_queue_when_unused=5.0, manual_ack=True, add_events=True, confirm=True)
        self.assertEqual(mqks.state['consumers'][worker][consumer_id], 'q1 e2 e3 e4 --delete-queue-when-unused=5.0 --manual-ack')
        self.assertEqual(mqks._eval("' '.join(sorted(state.events_by_queues['q1']))", worker=worker), 'e2 e3 e4')

        # Cleanup:

        mqks.delete_consumer(consumer_id, confirm=True)
        mqks.delete_queue('q1', confirm=True)
