import unittest
import query

class TestQuery(unittest.TestCase):
        
    def test_partial_datetime_match(self):
        self.assertTrue(query.partial_datetime_match('%Y-%m-%dT%H:%M:%SZ', '2011'))
        self.assertTrue(query.partial_datetime_match('%Y-%m-%dT%H:%M:%SZ', '2011-12-01'))
        self.assertFalse(query.partial_datetime_match('%Y-%m-%dT%H:%M:%SZ', '1-2-34'))
        self.assertFalse(query.partial_datetime_match('%Y-%m-%dT%H:%M:%SZ', '20110101'))
        self.assertFalse(query.partial_datetime_match('%Y-%m-%dT%H:%M:%SZ', '20091010'))

if __name__ == '__main__':
    unittest.main()
