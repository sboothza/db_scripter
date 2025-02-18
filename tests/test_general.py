import unittest

from sb_serializer import Naming, HardSerializer

from src.db_scripter.database_objects import View
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

    def test_dict(self):
        word = "ufnGetAccountingEndDate"
        naming = Naming("S:\\src\\pvt\\db_scripter\\dictionary.txt", "S:\\src\\pvt\\db_scripter\\bigworddictionary.txt")
        name = naming.string_to_name(word)
        print(name.name)

    def test_view(self):
        naming = Naming("S:\\src\\pvt\\db_scripter\\dictionary.txt", "S:\\src\\pvt\\db_scripter\\bigworddictionary.txt")
        serializer = HardSerializer(naming=naming)
        json = ""
        with open("test_view.json", 'r', encoding="utf-8") as file:
            json = file.read()

        view = serializer.de_serialize(json, View)
        print(view.name)
