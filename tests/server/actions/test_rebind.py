"""
Test MQKS Server Rebind
"""

### import

from mqks.tests.cases import MqksTestCase

### TestRebind

class TestRebind(MqksTestCase):

    ### test full rebind

    def test_full_rebind(self):
        client = self.get_simple_client()
        # consume
        consumer_id = client.send('consume --confirm q1 e1')
        self.assertEqual(client.get_response(consumer_id), 'ok ')
        # publish message
        publish_id = client.send('publish e1 1')
        # get message
        msg = client.get_response(consumer_id).split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])
        # get no message
        msg = client.get_response(consumer_id, timeout=0.1)
        self.assertTrue(msg is None)

        ## rebind
        rebind_id = client.send('rebind --confirm q1 e2')
        self.assertEqual(client.get_response(rebind_id), 'ok ')

        # publish message to old event
        client.send('publish e1 1')
        # get no message
        msg = client.get_response(consumer_id, timeout=0.1)
        self.assertTrue(msg is None, msg)

        # publish message to new event
        publish_id = client.send('publish e2 2')
        # get message
        msg = client.get_response(consumer_id).split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])
        self.assertEqual(msg[3], '2', msg[3])

        ## delete

        request_id = client.send('delete_consumer --confirm {}'.format(consumer_id))
        self.assertEqual(client.get_response(request_id), 'ok ')

        request_id = client.send('delete_queue --confirm q1')
        self.assertEqual(client.get_response(request_id), 'ok ')

    ### test add rebind

    def test_add_rebind(self):
        client = self.get_simple_client()
        # consume
        consumer_id = client.send('consume --confirm q1 e1')
        self.assertEqual(client.get_response(consumer_id), 'ok ')
        # publish message
        publish_id = client.send('publish e1 1')
        # get message
        msg = client.get_response(consumer_id).split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])

        ## publish message to not subscribed event
        client.send('publish e2 1')
        # get no message
        msg = client.get_response(consumer_id, timeout=0.1)
        self.assertTrue(msg is None)

        ## rebind
        rebind_id = client.send('rebind --confirm q1 --add e2 e3')
        self.assertEqual(client.get_response(rebind_id), 'ok ')

        # publish message to old event
        publish_id = client.send('publish e1 1')
        # get message
        msg = client.get_response(consumer_id).split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])

        # publish message to new events
        publish_id = client.send('publish e2 2')
        # get message
        msg = client.get_response(consumer_id).split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])
        self.assertEqual(msg[3], '2', msg[3])
        # publish message
        publish_id = client.send('publish e3 3')
        # get message
        msg = client.get_response(consumer_id).split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])
        self.assertEqual(msg[3], '3', msg[3])

        ## delete

        request_id = client.send('delete_consumer --confirm {}'.format(consumer_id))
        self.assertEqual(client.get_response(request_id), 'ok ')

        request_id = client.send('delete_queue --confirm q1')
        self.assertEqual(client.get_response(request_id), 'ok ')

    ### test remove rebind

    def test_remove_rebind(self):
        client = self.get_simple_client()
        # consume
        consumer_id = client.send('consume --confirm q1 e1 e2 e3')
        self.assertEqual(client.get_response(consumer_id), 'ok ')
        # publish message
        publish_id = client.send('publish e2 1')
        # get message
        msg = client.get_response(consumer_id).split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])

        ## rebind
        rebind_id = client.send('rebind --confirm q1 --remove e2')
        self.assertEqual(client.get_response(rebind_id), 'ok ')

        # publish message to removed event
        client.send('publish e2 1')
        # get no message
        msg = client.get_response(consumer_id, timeout=0.1)
        self.assertTrue(msg is None)

        # publish message exists events
        publish_id = client.send('publish e1 1')
        # get message
        msg = client.get_response(consumer_id).split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])
        self.assertEqual(msg[3], '1', msg[3])
        # publish message
        publish_id = client.send('publish e3 3')
        # get message
        msg = client.get_response(consumer_id).split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])
        self.assertEqual(msg[3], '3', msg[3])

        ## delete

        request_id = client.send('delete_consumer --confirm {}'.format(consumer_id))
        self.assertEqual(client.get_response(request_id), 'ok ')

        request_id = client.send('delete_queue --confirm q1')
        self.assertEqual(client.get_response(request_id), 'ok ')

    ### test remove mask rebind

    def test_remove_mask_rebind(self):
        client = self.get_simple_client()
        # consume
        consumer_id = client.send('consume --confirm q1 e1 e2.abc.c e2.abc.u e2.efg.c e2.efg.u e3.123.c e3.123.u')
        self.assertEqual(client.get_response(consumer_id), 'ok ')
        # publish message
        publish_id = client.send('publish e1 1')
        # get message
        msg = client.get_response(consumer_id).split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])
        # publish message
        publish_id = client.send('publish e2.abc.u 2')
        # get message
        msg = client.get_response(consumer_id).split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])
        self.assertEqual(msg[3], '2', msg[3])
        # publish message
        publish_id = client.send('publish e2.efg.u 3')
        # get message
        msg = client.get_response(consumer_id).split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])
        self.assertEqual(msg[3], '3', msg[3])
          # publish message
        publish_id = client.send('publish e3.123.u 4')
        # get message
        msg = client.get_response(consumer_id).split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])
        self.assertEqual(msg[3], '4', msg[3])

        ## rebind
        rebind_id = client.send('rebind --confirm q1 --remove-mask e2.*.u e2.*.c')
        self.assertEqual(client.get_response(rebind_id), 'ok ')

        # publish message to removed event
        client.send('publish e2.abc.c 1')
        # get no message
        msg = client.get_response(consumer_id, timeout=0.1)
        self.assertTrue(msg is None, msg)
        # publish message to removed event
        client.send('publish e2.abc.u 1')
        # get no message
        msg = client.get_response(consumer_id, timeout=0.1)
        self.assertTrue(msg is None)
        # publish message to removed event
        client.send('publish e2.efg.c 1')
        # get no message
        msg = client.get_response(consumer_id, timeout=0.1)
        self.assertTrue(msg is None)
        # publish message to removed event
        client.send('publish e2.efg.u 1')
        # get no message
        msg = client.get_response(consumer_id, timeout=0.1)
        self.assertTrue(msg is None)

        # publish message exists events
        publish_id = client.send('publish e1 1')
        # get message
        msg = client.get_response(consumer_id).split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])
        self.assertEqual(msg[3], '1', msg[3])
        # publish message
        publish_id = client.send('publish e3.123.c 2')
        # get message
        msg = client.get_response(consumer_id).split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])
        self.assertEqual(msg[3], '2', msg[3])
        # publish message
        publish_id = client.send('publish e3.123.u 3')
        # get message
        msg = client.get_response(consumer_id).split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])
        self.assertEqual(msg[3], '3', msg[3])

        ## rebind remove by not matched mask
        rebind_id = client.send('rebind --confirm q1 --remove-mask e3.*')
        self.assertEqual(client.get_response(rebind_id), 'ok ')

        # publish message
        publish_id = client.send('publish e3.123.u 4')
        # get message
        msg = client.get_response(consumer_id).split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])
        self.assertEqual(msg[3], '4', msg[3])

        ## rebind remove undefined events
        rebind_id = client.send('rebind --confirm q1 --remove-mask e1000.*.u.*')
        self.assertEqual(client.get_response(rebind_id), 'ok ')

        ## delete

        request_id = client.send('delete_consumer --confirm {}'.format(consumer_id))
        self.assertEqual(client.get_response(request_id), 'ok ')

        request_id = client.send('delete_queue --confirm q1')
        self.assertEqual(client.get_response(request_id), 'ok ')

    ### test mixed rebind

    def test_mixed_rebind(self):
        client = self.get_simple_client()
        # consume
        consumer_id = client.send('consume --confirm q1 e1 e2 e3')
        self.assertEqual(client.get_response(consumer_id), 'ok ')
        # publish message
        publish_id = client.send('publish e1 1')
        # get message
        msg = client.get_response(consumer_id).split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])
        # get no message
        msg = client.get_response(consumer_id, timeout=0.1)
        self.assertTrue(msg is None)

        ## rebind add
        rebind_id = client.send('rebind --confirm q1 --add e4 e5')
        self.assertEqual(client.get_response(rebind_id), 'ok ')

        # publish message
        publish_id = client.send('publish e4 4')
        # get message
        msg = client.get_response(consumer_id).split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])
        self.assertEqual(msg[3], '4', msg[3])

        ## rebind remove
        rebind_id = client.send('rebind --confirm q1 --remove e4')
        self.assertEqual(client.get_response(rebind_id), 'ok ')

        # publish message
        client.send('publish e4 4')
        # get no message
        msg = client.get_response(consumer_id, timeout=0.1)
        self.assertTrue(msg is None)
        # publish message
        publish_id = client.send('publish e5 5')
        # get message
        msg = client.get_response(consumer_id).split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])
        self.assertEqual(msg[3], '5', msg[3])

        ## delete

        request_id = client.send('delete_consumer --confirm {}'.format(consumer_id))
        self.assertEqual(client.get_response(request_id), 'ok ')

        request_id = client.send('delete_queue --confirm q1')
        self.assertEqual(client.get_response(request_id), 'ok ')

    ### test remove undefined event

    def test_remove_undefined_event(self):
        client = self.get_simple_client()
        # consume
        consumer_id = client.send('consume --confirm q1 e1 e2 e3')
        self.assertEqual(client.get_response(consumer_id), 'ok ')
        # publish message
        publish_id = client.send('publish e2 1')
        # get message
        msg = client.get_response(consumer_id).split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])

        ## rebind
        rebind_id = client.send('rebind --confirm q1 --remove e5')
        self.assertEqual(client.get_response(rebind_id), 'ok ')

        ## delete

        request_id = client.send('delete_consumer --confirm {}'.format(consumer_id))
        self.assertEqual(client.get_response(request_id), 'ok ')

        request_id = client.send('delete_queue --confirm q1')
        self.assertEqual(client.get_response(request_id), 'ok ')

    ### test add to undefined queue

    def test_add_to_undefined_queue(self):
        client = self.get_simple_client()
        # consume
        consumer_id = client.send('consume --confirm q1 e1 e2 e3')
        self.assertEqual(client.get_response(consumer_id), 'ok ')
        # publish message
        publish_id = client.send('publish e2 1')
        # get message
        msg = client.get_response(consumer_id).split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])

        ## rebind
        rebind_id = client.send('rebind --confirm q2 --add e5')
        self.assertEqual(client.get_response(rebind_id), 'ok ')

        # publish
        client.send('publish e5 1')
        # get no message
        msg = client.get_response(consumer_id, timeout=0.1)
        self.assertTrue(msg is None)

        # consume2
        consumer_id2 = client.send('consume --confirm q2 e6')
        self.assertEqual(client.get_response(consumer_id2), 'ok ')

        # publish
        client.send('publish e5 1')
        # get no message
        msg = client.get_response(consumer_id2, timeout=0.1)
        self.assertTrue(msg is None)

        # publish message
        publish_id = client.send('publish e6 1')
        # get message
        msg = client.get_response(consumer_id2).split(' ', 3)
        self.assertEqual(msg[0], 'ok', msg[0])
        self.assertEqual(msg[1], publish_id, msg[1])

        ## delete

        request_id = client.send('delete_consumer --confirm {}'.format(consumer_id))
        self.assertEqual(client.get_response(request_id), 'ok ')
        request_id = client.send('delete_consumer --confirm {}'.format(consumer_id2))
        self.assertEqual(client.get_response(request_id), 'ok ')

        request_id = client.send('delete_queue --confirm q1')
        self.assertEqual(client.get_response(request_id), 'ok ')
        request_id = client.send('delete_queue --confirm q2')
        self.assertEqual(client.get_response(request_id), 'ok ')
