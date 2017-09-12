"""
Test MQKS client: manual ack
"""

### import

import gevent

from mqks.client import mqks
from mqks.tests.cases import Mock, MqksTestCase

### test

class TestManualAck(MqksTestCase):

    def test_manual_ack(self):

        # Consume:

        on_msg = Mock()
        consumer_id = mqks.consume('q1', ['e1'], on_msg, manual_ack=True, confirm=True)

        # Publish:

        mqks.publish('e1', 'd1', confirm=True)

        # Original message:

        if not on_msg.called:
            gevent.sleep(0.1)
        self.assertEqual(on_msg.called, 1)

        msg = on_msg.args[0]
        self.assertEqual(msg['data'], 'd1')
        self.assertNotIn('retry', msg)

        # Delete consumer without ack, consume again:

        mqks.delete_consumer(consumer_id, confirm=True)
        consumer_id = mqks.consume('q1', ['e1'], on_msg, manual_ack=True, confirm=True)

        # Retried message:

        if on_msg.called == 1:
            gevent.sleep(0.1)
        self.assertEqual(on_msg.called, 2)

        msg = on_msg.args[0]
        self.assertEqual(msg['data'], 'd1')
        self.assertEqual(msg['retry'], '1')

        # Ack, delete_consumer, consume, no retries:

        msg['ack'](confirm=True)
        mqks.delete_consumer(consumer_id, confirm=True)
        consumer_id = mqks.consume('q1', ['e1'], on_msg, manual_ack=True, confirm=True)

        gevent.sleep(0.5)
        self.assertEqual(on_msg.called, 2)

        # Publish, get original, reject, no other consumers, get retry:

        mqks.publish('e1', 'd2', confirm=True)

        if on_msg.called == 2:
            gevent.sleep(0.1)
        self.assertEqual(on_msg.called, 3)

        msg = on_msg.args[0]
        self.assertEqual(msg['data'], 'd2')
        self.assertNotIn('retry', msg)

        msg['reject'](confirm=True)

        if on_msg.called == 3:
            gevent.sleep(0.1)
        self.assertEqual(on_msg.called, 4)

        msg = on_msg.args[0]
        self.assertEqual(msg['data'], 'd2')
        self.assertEqual(msg['retry'], '1')

        # Reject all, no other consumers, get retry:

        mqks.reject_all(consumer_id, confirm=True)

        if on_msg.called == 4:
            gevent.sleep(0.1)
        self.assertEqual(on_msg.called, 5)

        msg = on_msg.args[0]
        self.assertEqual(msg['data'], 'd2')
        self.assertEqual(msg['retry'], '2')

        # Ack all, delete_consumer, consume, no retries:

        mqks.ack_all(consumer_id, confirm=True)
        mqks.delete_consumer(consumer_id, confirm=True)
        consumer_id = mqks.consume('q1', ['e1'], on_msg, manual_ack=True, confirm=True)

        gevent.sleep(0.5)
        self.assertEqual(on_msg.called, 5)

        # Cleanup:

        mqks.delete_consumer(consumer_id, confirm=True)
        mqks.delete_queue('q1', confirm=True)
