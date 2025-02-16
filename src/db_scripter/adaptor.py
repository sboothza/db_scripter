from typing import List
from sb_serializer import Naming, HardSerializer
from database_objects import Table, Database, FieldType, KeyType


class Adaptor(object):
    def __init__(self, connection: str, naming: Naming):
        self.connection = connection
        self.naming = naming

    def import_schema(self, db_name: str = None, options: {} = None) -> Database:
        ...

    def write_schema(self, database: Database, path: str):
        ...

    @staticmethod
    def generate_schema_definition(database: Database, definition_file: str, serializer: HardSerializer):
        with open(definition_file, 'w') as output_file:
            output_file.write(serializer.serialize(database, True))
            output_file.flush()

    @staticmethod
    def import_definition(definition_file: str, naming: Naming, serializer: HardSerializer) -> Database:
        text = ""
        with open(definition_file, 'r') as input_file:
            lines = input_file.readlines()
            for line in lines:
                text = text + line

        database = serializer.de_serialize(text, naming)
        Adaptor._process_foreign_keys(database)
        return database

    @staticmethod
    def _process_foreign_keys(database: Database):
        for foreign_table in database.tables:
            for foreign_key in [key for key in foreign_table.keys if key.key_type == KeyType.ForeignKey]:
                primary_table = database.get_table(foreign_key.primary_table)
                primary_table.foreign_keys.append(foreign_key)

    @staticmethod
    def _add_dependant_tables(database: Database, table: Table, table_list: List[Table]):
        if table not in table_list:
            for fk in [key for key in table.keys if key.key_type == KeyType.ForeignKey]:
                primary_table = database.get_table(fk.primary_table)
                Adaptor._add_dependant_tables(database, primary_table, table_list)
            if table not in table_list:
                table_list.append(table)

    @staticmethod
    def get_ordered_table_list(database: Database) -> List[Table]:
        # find references and push in front
        tables: List[Table] = []
        for table in database.tables:
            Adaptor._add_dependant_tables(database, table, tables)

        return tables

    def generate_create_script(self, table: Table) -> str:
        ...

    def get_field_type(self, field_type: FieldType) -> str:
        ...

    def escape_field_list(self, values: List[str]) -> List[str]:
        ...
