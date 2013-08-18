import unittest
import template_query as q

class TestTemplateQuery(unittest.TestCase):

    def test_noescape(self):
        self.assertEqual(q.escape_variables("abcd\n"), ["abcd"])

    def test_noinclude(self):
        self.assertEqual(q.escape_variables("abcd<noinclude>efghij</noinclude>"), ["abcd"])
        self.assertEqual(q.escape_variables("abcd\n<noinclude>\nefghij\n</noinclude>\n"), ["abcd"])

if __name__ == '__main__':
    unittest.main()

