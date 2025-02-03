from typing import List
from sb_serializer import Naming, HardSerializer
from database_objects import Table, Database, FieldType, KeyType


class Adaptor(object):
    def __init__(self, connection: str, naming: Naming):
        self.connection = connection
        self.naming = naming

    def import_schema(self, db_name: str) -> Database:
        pass

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

    def escape_field_list(self, values: List[str]) -> List[str]:
        pass

    def generate_drop_script(self, table: Table) -> str:
        pass

    def generate_create_script(self, table: Table) -> str:
        pass

    def generate_table_exists_script(self, table: Table, db_name: str) -> str:
        pass

    def generate_count_script(self, table: Table) -> str:
        pass

    def generate_insert_script(self, table: Table) -> str:
        pass

    def generate_update_script(self, table: Table) -> str:
        pass

    def generate_delete_script(self, table: Table) -> str:
        pass

    def generate_fetch_by_id_script(self, table: Table) -> str:
        pass

    def generate_item_exists_script(self, table: Table) -> str:
        pass

    def get_field_type(self, field_type: FieldType) -> str:
        pass

    def must_remap_field(self, field_type: FieldType) -> tuple[bool, FieldType]:
        pass

    def replace_parameters(self, query: str) -> str:
        pass

    def build_selection_list(self, fields: list) -> str:
        pass
