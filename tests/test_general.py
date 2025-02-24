import unittest

from sb_serializer import Naming, HardSerializer

from src.db_scripter.database_objects import View
from src.db_scripter.options import Options
from src.db_scripter.query_parser import Parser, SqlSelectToken, SqlStarToken, SqlFromToken, SqlLiteralToken
from tests.common import naming, serializer


class TestGeneral(unittest.TestCase):

    def setUp(self):
        ...

    def test_options(self):
        options = Options("key1=value1;key2=value2;key3=value3")

        self.assertEqual(options["key1"], "value1")
        self.assertEqual(options["key2"], "value2")
        self.assertEqual(options["key3"], "value3")
        self.assertEqual(options["key4", "empty"], "empty")

    def test_dict(self):
        word = "ufnGetAccountingEndDate"
        # naming = Naming("S:\\src\\pvt\\db_scripter\\dictionary.txt", "S:\\src\\pvt\\db_scripter\\bigworddictionary.txt")
        name = naming.string_to_name(word)
        print(name.name)

    def test_view(self):
        # naming = Naming("S:\\src\\pvt\\db_scripter\\dictionary.txt", "S:\\src\\pvt\\db_scripter\\bigworddictionary.txt")
        # serializer = HardSerializer(naming=naming)
        json = ""
        with open("test_view.json", 'r', encoding="utf-8") as file:
            json = file.read()

        view = serializer.de_serialize(json, View)
        print(view.name)

    def test_parser_chars(self):
        self.assertTrue("t".isalnum())
        self.assertTrue("3.4".isdecimal())
        self.assertFalse(" ".isalnum())

    def test_parser_basic(self):
        parser = Parser("select * from bob")
        self.assertTrue(len(parser.tokens) == 4)
        self.assertTrue(type(parser.tokens[0]) == SqlSelectToken)
        self.assertTrue(type(parser.tokens[1]) == SqlStarToken)
        self.assertTrue(type(parser.tokens[2]) == SqlFromToken)
        self.assertTrue(type(parser.tokens[3]) == SqlLiteralToken)

    def test_parser_mssql(self):
        parser = Parser("select [name] from [dbo].[bob] where [name]<>'bob'")
        self.assertTrue(len(parser.tokens) == 8)

    def test_list(self):
        l1 = [1, 2, 3]
        l2 = [1, 2, 3]
        l3 = [1, 3, 2]

        self.assertTrue(l1 == l2)
        self.assertTrue(l1 == l3)
