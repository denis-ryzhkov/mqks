"""
Test MQKS Server Reject
"""

### import

from mqks.tests.cases import MqksTestCase

### TestReject

class TestReject(MqksTestCase):

    ### test reject

    def test_reject(self):
        # connect
        q1_client1 = self.get_simple_client('q1')
        q1_client2 = self.get_simple_client('q1')
        # consume
        consumer_id1 = q1_client1.send('consume q1 e1 --manual-ack')
        consumer_id2 = q1_client2.send('consume --confirm q1 e1 --manual-ack')
        self.assertEqual(q1_client2.get_response(consumer_id2), 'ok ')
        # publish message
        publish_id = self.get_simple_client('e1').send('publish e1 1')
        # get message
        msg = q1_client1.get_response(consumer_id1, timeout=0.1)
        q1_client1_got_msg = msg is not None
        if not q1_client1_got_msg:
            msg = q1_client2.get_response(consumer_id2, timeout=0.1)
        msg = msg.split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])
        self.assertEqual(msg[2], 'event=e1', msg[2])
        self.assertEqual(msg[3], '1', msg[3])
        # reject
        (q1_client1 if q1_client1_got_msg else q1_client2).send('reject {} {}'.format((consumer_id1 if q1_client1_got_msg else consumer_id2), publish_id))

        # get message
        msg = q1_client1.get_response(consumer_id1, timeout=0.1)
        q1_client1_got_msg = msg is not None
        if not q1_client1_got_msg:
            msg = q1_client2.get_response(consumer_id2, timeout=0.1)
        msg = msg.split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])
        self.assertEqual(msg[2], 'event=e1,retry=1', msg[2])
        self.assertEqual(msg[3], '1', msg[3])
        # ack
        (q1_client1 if q1_client1_got_msg else q1_client2).send('ack {} {}'.format((consumer_id1 if q1_client1_got_msg else consumer_id2), publish_id))

        msg = q1_client1.get_response(consumer_id1, timeout=0.1)
        self.assertTrue(msg is None)
        msg = q1_client2.get_response(consumer_id2, timeout=0.1)
        self.assertTrue(msg is None)

        # delete
        request_id = q1_client1.send('delete_queue --confirm q1')
        self.assertEqual(q1_client1.get_response(request_id), 'ok ')
