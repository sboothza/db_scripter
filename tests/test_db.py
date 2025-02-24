import unittest

from src.db_scripter.database_objects import Database, Table, Key, KeyType, QualifiedName, Dependancy
from tests.common import naming


class TestDb(unittest.TestCase):

    def setUp(self):
        ...

    def test_dependancies(self):
        # naming = Naming("..\\dictionary.txt", "..\\bigworddictionary.txt")
        db = Database(naming.string_to_name("test"))
        table_cust = Table(QualifiedName.create("", "customer"))
        db.tables.append(table_cust)
        table_addr = Table(QualifiedName.create("", "address"))
        fk = Key(None, KeyType.ForeignKey)
        fk.primary_table = table_addr.name
        fk.referenced_table = table_cust.name
        table_addr.foreign_keys.append(fk)
        db.tables.append(table_addr)
        db.dependancies.append(Dependancy(table_addr.name, table_cust.name, "Table"))
        db.dependancies.append(Dependancy(QualifiedName.create("", "table1"), table_cust.name, "Table"))

        print("dependancies")
        for d in db.dependancies:
            print(d)

        db.finalise()
        db.clean_dependancies()

        print("dependancies")
        for d in db.dependancies:
            print(d)
