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
        client = self.get_simple_client()
        # subscribe
        client.send('subscribe q1 e1')
        # consume
        consumer_id = client.send('consume q1')
        # publish message
        publish_id = client.send('publish e1 1')
        # get message
        msg = client.get_response(consumer_id).split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])
        self.assertEqual(msg[2], 'event=e1', msg[2])
        self.assertEqual(msg[3], '1', msg[3])

        # delete consumer
        client.send('delete_consumer {}'.format(consumer_id))

        # consume
        consumer_id = client.send('consume q1')
        msg = client.get_response(consumer_id, timeout=0.1)
        self.assertTrue(msg is None)

        # delete queue
        client.send('delete_queue q1')
        # delete consumer
        client.send('delete_consumer {}'.format(consumer_id))

    ### test manual ack

    def test_manual_ack(self):
        client1 = self.get_simple_client()
        # subscribe
        client1.send('subscribe q1 e1')
        # consume
        consumer_id = client1.send('consume q1 --manual-ack')
        # publish message
        publish_id = client1.send('publish e1 1')
        # get message
        msg = client1.get_response(consumer_id).split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])
        self.assertEqual(msg[2], 'event=e1', msg[2])
        self.assertEqual(msg[3], '1', msg[3])
        # close client
        client1.close()

        # connect second client
        client2 = self.get_simple_client()
        # subsrcibe
        client2.send('subscribe q1 e1')
        # consume
        consumer_id = client2.send('consume q1 --manual-ack')
        # get message
        msg = client2.get_response(consumer_id).split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])
        self.assertEqual(msg[2], 'event=e1,retry=1', msg[2])
        self.assertEqual(msg[3], '1', msg[3])
        # ack
        client2.send('ack {} {}'.format(consumer_id, publish_id))
        # close client
        client2.close()

        # connect third client
        client3 = self.get_simple_client()
        # subsrcibe
        client3.send('subscribe q1 e1')
        # consume
        consumer_id = client3.send('consume q1 --manual-ack')
        # get message
        msg = client3.get_response(consumer_id, timeout=0.1)
        self.assertTrue(msg is None)

        # delete queue
        client3.send('delete_queue q1')
        client3.close()

    ### test manual ack two consumers

    def test_manual_ack_two_consumers(self):
        # connect
        client1 = self.get_simple_client()
        client2 = self.get_simple_client()
        # subscribe
        client1.send('subscribe q1 e1')
        client2.send('subscribe q2 e1')
        # delete queue
        client1.send('delete_queue q1 --when-unused=5')
        client2.send('delete_queue q2 --when-unused=5')
        # consume
        consumer_id1 = client1.send('consume q1 --manual-ack')
        consumer_id2 = client2.send('consume q2 --manual-ack')
        time.sleep(0.1)
        # publish message
        publish_id = client1.send('publish e1 1')
        # get message
        msg = client1.get_response(consumer_id1).split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])
        self.assertEqual(msg[2], 'event=e1', msg[2])
        self.assertEqual(msg[3], '1', msg[3])

        # get message
        msg = client2.get_response(consumer_id2).split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])
        self.assertEqual(msg[2], 'event=e1', msg[2])
        self.assertEqual(msg[3], '1', msg[3])

        # ack first client
        client1.send('ack {} {}'.format(consumer_id1, publish_id))
        # close client2
        client2.close()

        # connect
        client3 = self.get_simple_client()
        # subsrcibe
        client3.send('subscribe q2 e1')
        # delete queue
        client3.send('delete_queue q2 --when-unused=5')
        # consume
        consumer_id3 = client3.send('consume q2 --manual-ack')
        # get message
        msg = client3.get_response(consumer_id3).split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])
        self.assertEqual(msg[2], 'event=e1,retry=1', msg[2])
        self.assertEqual(msg[3], '1', msg[3])
        # ack
        client3.send('ack {} {}'.format(consumer_id3, publish_id))

        # get message
        msg = client3.get_response(consumer_id3, timeout=0.1)
        self.assertTrue(msg is None)

        # delete queue
        client3.send('delete_queue q1')
        client3.send('delete_queue q2')
        # close
        client1.close()
        client3.close()
