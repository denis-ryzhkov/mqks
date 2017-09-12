"""
Test MQKS client: error
"""

### import

from mqks.client import mqks
from mqks.tests.cases import MqksTestCase

### test

class TestError(MqksTestCase):

    def test_error(self):

        try:
            mqks._eval('1/0')

        except Exception as e:
            # Client errors should contain only secure error id to find in server detailed crits.
            # Exception: 201709070912043734522yx6 error 2017090709120437422267aw
            parts = str(e).split(' ')
            self.assertEqual(len(parts), 3)
            self.assertEqual(len(parts[0]), 24)
            self.assertEqual(parts[1], 'error')
            self.assertEqual(len(parts[2]), 24)

        else:
            self.assertTrue(False, 'Should raise an Exception')
