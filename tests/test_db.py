import unittest

from sb_serializer import Naming

from src.db_scripter.database_objects import Database, Table, Key, KeyType, QualifiedName, Dependancy


class TestDb(unittest.TestCase):

    def setUp(self):
        ...

    def test_dependancies(self):
        naming = Naming("..\\dictionary.txt", "..\\bigworddictionary.txt")
        db = Database(naming.string_to_name("test"))
        table_cust = Table(QualifiedName(naming.string_to_name(""),naming.string_to_name("customer")))
        db.tables.append(table_cust)
        table_addr = Table(QualifiedName(naming.string_to_name(""), naming.string_to_name("address")))
        fk = Key(None, KeyType.ForeignKey)
        fk.primary_table = table_addr
        fk.referenced_table = table_cust
        table_addr.foreign_keys.append(fk)
        db.tables.append(table_addr)
        db.dependancies.append(Dependancy(table_addr.name, table_cust.name,  "Table"))
        db.dependancies.append(Dependancy(QualifiedName(naming.string_to_name(""), naming.string_to_name("table1")), table_cust.name, "Table"))

        print("dependancies")
        for d in db.dependancies:
            print(d)

        db.finalise()
        db.clean_dependancies()

        print("dependancies")
        for d in db.dependancies:
            print(d)


