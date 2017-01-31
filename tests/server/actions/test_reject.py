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
        client1 = self.get_simple_client()
        client2 = self.get_simple_client()
        # consume
        consumer_id1 = client1.send('consume q1 e1 --manual-ack')
        consumer_id2 = client2.send('consume --confirm q1 e1 --manual-ack')
        self.assertEqual(client2.get_response(consumer_id2), 'ok ')
        # publish message
        publish_id = client1.send('publish e1 1')
        # get message
        msg = client1.get_response(consumer_id1, timeout=0.1)
        client1_got_msg = msg is not None
        if not client1_got_msg:
            msg = client2.get_response(consumer_id2, timeout=0.1)
        msg = msg.split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])
        self.assertEqual(msg[2], 'event=e1', msg[2])
        self.assertEqual(msg[3], '1', msg[3])
        # reject
        (client1 if client1_got_msg else client2).send('reject {} {}'.format((consumer_id1 if client1_got_msg else consumer_id2), publish_id))

        # get message
        msg = client1.get_response(consumer_id1, timeout=0.1)
        client1_got_msg = msg is not None
        if not client1_got_msg:
            msg = client2.get_response(consumer_id2, timeout=0.1)
        msg = msg.split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])
        self.assertEqual(msg[2], 'event=e1,retry=1', msg[2])
        self.assertEqual(msg[3], '1', msg[3])
        # ack
        (client1 if client1_got_msg else client2).send('ack {} {}'.format((consumer_id1 if client1_got_msg else consumer_id2), publish_id))

        msg = client1.get_response(consumer_id1, timeout=0.1)
        self.assertTrue(msg is None)
        msg = client2.get_response(consumer_id2, timeout=0.1)
        self.assertTrue(msg is None)

        # delete
        request_id = client1.send('delete_queue --confirm q1')
        self.assertEqual(client1.get_response(request_id), 'ok ')
