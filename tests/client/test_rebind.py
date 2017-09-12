"""
Test MQKS client: rebind
"""

### import

from mqks.client import mqks
from mqks.tests.cases import Mock, MqksTestCase

### test

class TestRebind(MqksTestCase):

    def test_rebind(self):

        # Consume:

        on_msg = Mock()
        consumer_id = mqks.consume('q1', ['e1', 'e2', 'u.42.e1', 'u.43.e1'], on_msg, confirm=True)

        # "replace" was tested in "test_update_consumer".
        # Testing other args here:

        mqks.rebind('q1', remove=['e2'], add=['e3', 'e4'], remove_mask=['u.*.e1'], confirm=True)

        worker = mqks.get_worker('q1')
        self.assertEqual(mqks.state['consumers'][worker][consumer_id], 'q1 e1 e3 e4')

        # Cleanup:

        mqks.delete_consumer(consumer_id, confirm=True)
        mqks.delete_queue('q1', confirm=True)
