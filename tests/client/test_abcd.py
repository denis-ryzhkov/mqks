"""
Test MQKS client: hello world from "spec.txt"
Starring: Alice, Bob, Charlie, Dave
Test name was composed to execute this test first - in alphabetical order.
"""

### import

import gevent

from mqks.client import mqks
from mqks.tests.cases import Mock, MqksTestCase

### test

class TestABCD(MqksTestCase):

    def test_abcd(self):

        # Two consumers of the same queue - Alice and Bob:

        alice = Mock()
        mqks.consume('greetings', ['hi', 'hello'], alice, confirm=True)

        bob = Mock()
        mqks.consume('greetings', ['hi', 'hello'], bob, confirm=True)

        # Consumer of another queue - Charlie:

        charlie = Mock()
        mqks.consume('greetings-and-byes', ['hi', 'hello', 'bye', 'good-bye'], charlie, confirm=True)

        # One publish from Dave:

        dave_msg_id = mqks.publish('hello', 'world', confirm=True)

        # One and only one of Alice and Bob should get the message from Dave:

        if not alice.called or bob.called:
            gevent.sleep(0.1)

        self.assertTrue(alice.called == 1 or bob.called == 1)
        self.assertFalse(alice.called and bob.called)

        on_msg = alice if alice.called else bob
        self.assertEqual(len(on_msg.args), 1)
        self.assertEqual(len(on_msg.kwargs), 0)

        msg = on_msg.args[0]
        self.assertIsInstance(msg, dict)

        self.assertIn('id', msg)
        self.assertEqual(msg['id'], dave_msg_id)

        self.assertIn('event', msg)
        self.assertEqual(msg['event'], 'hello')

        self.assertIn('data', msg)
        self.assertEqual(msg['data'], 'world')

        # Charlie should get message from Dave too:

        self.assertEqual(charlie.called, 1)
        msg = charlie.args[0]
        self.assertEqual(msg['id'], dave_msg_id)
        self.assertEqual(msg['event'], 'hello')
        self.assertEqual(msg['data'], 'world')
