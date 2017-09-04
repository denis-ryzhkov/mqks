"""
Test MQKS Server Ack
"""

### import

import time
from mqks.tests.cases import MqksTestCase

### TestAck

class TestAck(MqksTestCase):

    ### test auto ack

    def test_auto_ack(self):
        q1_client = self.get_simple_client('q1')
        # consume
        consumer_id = q1_client.send('consume --confirm q1 e1')
        self.assertEqual(q1_client.get_response(consumer_id), 'ok ')
        # publish message
        e1_client = self.get_simple_client('e1')
        publish_id = e1_client.send('publish e1 1')
        # get message
        msg = q1_client.get_response(consumer_id).split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])
        self.assertEqual(msg[2], 'event=e1', msg[2])
        self.assertEqual(msg[3], '1', msg[3])

        # delete consumer
        q1_client.send('delete_consumer {}'.format(consumer_id))

        # consume
        consumer_id = q1_client.send('consume --confirm q1 e1')
        self.assertEqual(q1_client.get_response(consumer_id), 'ok ')

        msg = q1_client.get_response(consumer_id, timeout=0.1)
        self.assertTrue(msg is None)

        # delete

        request_id = q1_client.send('delete_consumer --confirm {}'.format(consumer_id))
        self.assertEqual(q1_client.get_response(request_id), 'ok ')

        request_id = q1_client.send('delete_queue --confirm q1')
        self.assertEqual(q1_client.get_response(request_id), 'ok ')

    ### test manual ack

    def test_manual_ack(self):
        q1_client1 = self.get_simple_client('q1')
        # consume
        consumer_id = q1_client1.send('consume --confirm q1 e1 --manual-ack')
        self.assertEqual(q1_client1.get_response(consumer_id), 'ok ')
        # publish message
        e1_client = self.get_simple_client('e1')
        publish_id = e1_client.send('publish e1 1')
        # get message
        msg = q1_client1.get_response(consumer_id).split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])
        self.assertEqual(msg[2], 'event=e1', msg[2])
        self.assertEqual(msg[3], '1', msg[3])
        # close client
        q1_client1.close()

        # connect second client
        q1_client2 = self.get_simple_client('q1')
        # consume
        consumer_id = q1_client2.send('consume --confirm q1 e1 --manual-ack')
        self.assertEqual(q1_client2.get_response(consumer_id), 'ok ')
        # get message
        msg = q1_client2.get_response(consumer_id).split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])
        self.assertEqual(msg[2], 'event=e1,retry=1', msg[2])
        self.assertEqual(msg[3], '1', msg[3])
        # ack
        q1_client2.send('ack {} {}'.format(consumer_id, publish_id))
        # close client
        q1_client2.close()

        # connect third client
        q1_client3 = self.get_simple_client('q1')
        # consume
        consumer_id = q1_client3.send('consume --confirm q1 e1 --manual-ack')
        self.assertEqual(q1_client3.get_response(consumer_id), 'ok ')
        # get message
        msg = q1_client3.get_response(consumer_id, timeout=0.1)
        self.assertTrue(msg is None)

        # delete

        request_id = q1_client3.send('delete_queue --confirm q1')
        self.assertEqual(q1_client3.get_response(request_id), 'ok ')

        q1_client3.close()

    ### test manual ack two consumers

    def test_manual_ack_two_consumers(self):
        # connect
        q1_client1 = self.get_simple_client('q1')
        q2_client2 = self.get_simple_client('q2')
        # consume
        consumer_id1 = q1_client1.send('consume q1 e1 --manual-ack')
        consumer_id2 = q2_client2.send('consume --confirm q2 e1 --manual-ack')
        self.assertEqual(q2_client2.get_response(consumer_id2), 'ok ')
        # delete queue
        q1_client1.send('delete_queue q1 --when-unused=5')
        q2_client2.send('delete_queue q2 --when-unused=5')
        time.sleep(0.1)
        # publish message
        e1_client = self.get_simple_client('e1')
        publish_id = e1_client.send('publish e1 1')
        # get message
        msg = q1_client1.get_response(consumer_id1).split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])
        self.assertEqual(msg[2], 'event=e1', msg[2])
        self.assertEqual(msg[3], '1', msg[3])

        # get message
        msg = q2_client2.get_response(consumer_id2).split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])
        self.assertEqual(msg[2], 'event=e1', msg[2])
        self.assertEqual(msg[3], '1', msg[3])

        # ack first client
        q1_client1.send('ack {} {}'.format(consumer_id1, publish_id))
        # close q2_client2
        q2_client2.close()

        # connect
        q2_client3 = self.get_simple_client('q2')
        # consume
        consumer_id3 = q2_client3.send('consume --confirm q2 e1 --manual-ack')
        self.assertEqual(q2_client3.get_response(consumer_id3), 'ok ')
        # delete queue
        q2_client3.send('delete_queue q2 e1 --when-unused=5')
        # get message
        msg = q2_client3.get_response(consumer_id3).split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])
        self.assertEqual(msg[2], 'event=e1,retry=1', msg[2])
        self.assertEqual(msg[3], '1', msg[3])
        # ack
        q2_client3.send('ack {} {}'.format(consumer_id3, publish_id))

        # get message
        msg = q2_client3.get_response(consumer_id3, timeout=0.1)
        self.assertTrue(msg is None)

        # delete

        request_id = q1_client1.send('delete_queue --confirm q1')
        self.assertEqual(q1_client1.get_response(request_id), 'ok ')

        request_id = q2_client3.send('delete_queue --confirm q2')
        self.assertEqual(q2_client3.get_response(request_id), 'ok ')

        # close
        q1_client1.close()
        q2_client3.close()
