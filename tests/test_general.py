import unittest

from src.db_scripter.options import Options


class TestGeneral(unittest.TestCase):

    def setUp(self):
        ...

    def test_options(self):
        options = Options("key1=value1;key2=value2;key3=value3")

        self.assertEqual(options["key1"], "value1")
        self.assertEqual(options["key2"], "value2")
        self.assertEqual(options["key3"], "value3")
        self.assertEqual(options["key4", "empty"], "empty")