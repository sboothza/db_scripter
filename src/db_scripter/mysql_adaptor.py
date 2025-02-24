import re
from typing import List

import mysql.connector

from database_objects import Database, Table, KeyType, Key, Field, DataException, DatatypeException, UDDT
from adaptor import Adaptor
from src.db_scripter.common import naming
from src.db_scripter.database_objects import QualifiedName


class MySqlAdaptor(Adaptor):
    """ Connection string is mysql://user:pass@hostname/database """
    __blank_connection__ = "mysql://u:p@h/d"

    def __init__(self, connection):
        super().__init__(connection)

        match = re.match(r"mysql:\/\/(\w+):(\w+)@(\w+)\/(\w+)", self.connection)
        if match:
            self.user = match.group(1)
            self.password = match.group(2)
            self.hostname = match.group(3)
            self.database = match.group(4)
        else:
            raise DataException("Invalid connection string")

    def import_schema(self, db_name: str = None, options: {} = None) -> Database:
        connection = mysql.connector.connect(user=self.user, password=self.password, host=self.hostname,
                                             database=self.database)

        if db_name is None:
            db_name = self.database

        database = Database(naming.string_to_name(db_name))
        cursor = connection.cursor(buffered=True)
        print("Processing tables...")
        cursor.execute("select TABLE_NAME from INFORMATION_SCHEMA.tables where TABLE_SCHEMA = 'test' and "
                       "TABLE_TYPE = 'BASE TABLE'")
        for row in cursor.fetchall():
            table = Table(QualifiedName(naming.string_to_name(""), naming.string_to_name(row[0])))
            database.tables.append(table)

        for table in database.tables:
            print(f"Processing fields for {table.name}...")
            cursor.execute("select COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, EXTRA, IS_NULLABLE, "
                           "NUMERIC_PRECISION, NUMERIC_SCALE, COLUMN_DEFAULT  from INFORMATION_SCHEMA.columns where "
                           f"TABLE_SCHEMA = '{db_name}' and TABLE_NAME='{table.name}' order by ORDINAL_POSITION")

            for row in cursor.fetchall():
                field = Field(QualifiedName(naming.string_to_name(""), naming.string_to_name(str(row[0]))),
                              auto_increment=True if "auto_increment" in str(row[3]).lower() else False,
                              required=str(row[4]).lower() != "yes")
                self.get_field_type_defaults(row[1].decode("utf-8"), field, row[2] if row[2] is not None else 0, row[5],
                                             row[6], row[7])

                table.fields.append(field)

            print("Processing indexes and keys")

            cursor.execute("select fks.constraint_name as constraint_name, fks.referenced_table_name as primary_table, "
                           "group_concat(kcu.column_name order by position_in_unique_constraint separator ', ') as "
                           "local_columns, group_concat(kcu.referenced_column_name order by "
                           "position_in_unique_constraint separator ', ') as reference_columns, 0 as NON_UNIQUE, "
                           "'FOREIGN KEY' as type from information_schema.referential_constraints fks "
                           "join information_schema.key_column_usage kcu "
                           "on fks.constraint_schema = kcu.table_schema "
                           "and fks.table_name = kcu.table_name "
                           "and fks.constraint_name = kcu.constraint_name "
                           f"where fks.constraint_schema = '{db_name}' and fks.table_name = '{table.name}' "
                           "group by fks.constraint_name, fks.referenced_table_name "
                           "union "
                           "select s.INDEX_NAME as constraint_name, null as primary_table, "
                           "group_concat(s.COLUMN_NAME  order by s.SEQ_IN_INDEX separator ', ') as local_columns, "
                           "null as reference_columns, s.NON_UNIQUE, case when c.CONSTRAINT_TYPE = 'FOREIGN KEY' "
                           "then 'INDEX' when c.CONSTRAINT_TYPE is null then 'INDEX' else c.CONSTRAINT_TYPE end as "
                           "`type` from INFORMATION_SCHEMA.STATISTICS s left join "
                           "INFORMATION_SCHEMA.table_constraints c on s.TABLE_SCHEMA = c.TABLE_SCHEMA and "
                           "s.TABLE_NAME = c.TABLE_NAME and s.INDEX_NAME = c.CONSTRAINT_NAME "
                           f"where s.TABLE_SCHEMA = '{db_name}' and s.TABLE_NAME = '{table.name}' "
                           "group by s.INDEX_NAME, s.NON_UNIQUE, c.CONSTRAINT_TYPE ")
            for row in cursor.fetchall():
                key = Key(QualifiedName(naming.string_to_name(""), naming.string_to_name(row[0])))
                key.referenced_table = table.name
                key_type = str(row[5].lower())
                key.key_type = KeyType.get_keytype(key_type)

                key.fields = [str(f.strip()) for f in row[2].split(",")]

                if key.key_type == KeyType.ForeignKey:
                    key.primary_table = row[1]
                    key.primary_fields = [str(f.strip()) for f in row[3].split(",")]

                if key.key_type == KeyType.PrimaryKey:
                    table.pk = key
                else:
                    table.keys.append(key)

        connection.close()
        return database

    @staticmethod
    def get_field_size(field: Field) -> str:
        if field.generic_type == "string":
            return f"({field.size})"
        elif field.generic_type == "decimal":
            return f"({field.size},{field.scale})"
        return ""

    def escape_field_list(self, values: List[str]) -> List[str]:
        return [f"`{value}`" for value in values]

    def generate_drop_script(self, table: Table) -> str:
        return f"DROP TABLE `{table.name}`;"

    @staticmethod
    def get_field_default(field: Field) -> str:
        if field.generic_type == "string" or field.generic_type == "datetime":
            return f"'{field.default}'"

    def generate_create_script(self, table: Table, original_db_type: str) -> str:
        sql: list[str] = []
        for field in table.fields:
            sql.append(f"`{field.name}` {self.get_field_type(field, original_db_type)}"
                       f"{self.get_field_size(field)} {'NOT NULL' if field.required else 'NULL'}"
                       f"{' AUTO_INCREMENT' if field.auto_increment else ''}"
                       f"{' DEFAULT (' + self.get_field_default(field) + ')' if field.default else ''}")
        if table.pk:
            sql.append(f"PRIMARY KEY ({','.join(table.pk.fields)})")

        for fk in [key for key in table.keys if key.key_type == KeyType.ForeignKey]:
            sql.append(f"FOREIGN KEY ({','.join(self.escape_field_list(fk.fields))}) REFERENCES "
                       f"{fk.primary_table}({','.join(self.escape_field_list(fk.primary_fields))})")
        joiner = ',\n\t'
        result = f"CREATE TABLE `{table.name}` (\n\t{joiner.join(sql)}\n);\n"

        for key in table.keys:
            if key.key_type == KeyType.Unique:
                result += f"CREATE UNIQUE INDEX `{key.name}` ON {table.name} " \
                          f"({','.join(self.escape_field_list(key.fields))});\n"

            elif key.key_type == KeyType.Index:
                result += f"CREATE INDEX `{key.name}` ON {table.name} " \
                          f"({','.join(self.escape_field_list(key.fields))});\n"

        return result

    def generate_table_exists_script(self, table: Table, db_name: str) -> str:
        return f"select TABLE_NAME from INFORMATION_SCHEMA.tables where TABLE_SCHEMA = '{db_name}' and " \
               f"TABLE_TYPE = 'BASE TABLE' and TABLE_NAME = '{table.name}'"

    def generate_count_script(self, table: Table) -> str:
        return f"select count(*) from `{table.name}`"

    def generate_insert_script(self, table: Table) -> str:
        fields = [f.name for f in table.fields if not f.auto_increment]
        params = ", ".join([f"`{f}`" for f in fields])
        values = ", ".join([f"%({f})s" for f in fields])
        result = f"insert into `{table.name}` ({params}) values ({values});"
        return result

    def generate_update_script(self, table: Table) -> str:
        fields = [f.name for f in table.fields if not f.auto_increment and f.name not in table.pk.fields]
        update_list = [f"`{f}` = %({f})s" for f in fields]
        update = ", ".join(update_list)
        key_list = [f"`{f}` = %({f})s" for f in table.pk.fields]
        key = " and ".join(key_list)
        result = f"update `{table.name}` set {update} where {key};"
        return result

    def generate_delete_script(self, table: Table) -> str:
        key_list = [f"`{f}` = %({f})s" for f in table.pk.fields]
        key = " and ".join(key_list)
        result = f"delete from `{table.name}` where {key};"
        return result

    def generate_fetch_by_id_script(self, table: Table) -> str:
        field_list = [f"`{f.name}`" for f in table.fields]
        fields = ", ".join(field_list)
        key_list = [f"`{f}` = %({f})s" for f in table.pk.fields]
        key = " and ".join(key_list)
        result = f"select {fields} from `{table.name}` where {key};"
        return result

    def generate_item_exists_script(self, table: Table) -> str:
        key_list = [f"`{f}` = %({f})s" for f in table.pk.fields]
        key = " and ".join(key_list)
        result = f"select count(*) from `{table.name}` where {key};"
        return result

    @staticmethod
    def get_field_type_defaults(value: str, field: Field, size, precision, scale, default):
        value = value.lower()
        default = None if default is None else default.decode("utf-8")
        if value == "integer" or value == "int":
            field.generic_type = "integer"
            field.size = 4
        elif value == "bigint":
            field.generic_type = "integer"
            field.size = 8
        elif value == "tinyint":
            field.generic_type = "integer"
            field.size = 1
        elif value == "smallint":
            field.generic_type = "integer"
            field.size = 2
        elif value == "mediumint":
            field.generic_type = "integer"
            field.size = 3
        elif value == "float" or value == "real":
            field.generic_type = "float"
            field.size = 4
        elif value == "double":
            field.generic_type = "float"
            field.size = 8
        elif value == "boolean" or value == "bool":
            field.generic_type = "boolean"
            field.size = 1
        elif value == "decimal" or value == "money":
            field.generic_type = "decimal"
            field.size = precision
            field.scale = scale
        elif value == "string" or value == "varchar" or value == "char":
            field.generic_type = "string"
            field.size = size
        elif value == "datetime" or value == "date":
            field.generic_type = "datetime"
            field.size = 0
        elif value == "none" or value == "undefined":
            field.generic_type = "undefined"
            field.size = 0
        else:
            raise DatatypeException("Unknown field type {}".format(value))
        field.default = default

    def get_field_type(self, field: Field | UDDT, original_db_type: str) -> str:
        if original_db_type == "mysql" and field.native_type is not None:
            return field.native_type.name.raw()

        if field.generic_type == "integer":
            if field.size == 1:
                return "TINYINT"
            elif field.size == 2:
                return "SMALLINT"
            elif field.size == 3:
                return "MEDIUMINT"
            elif field.size == 4:
                return "INT"
            elif field.size == 8:
                return "BIGINT"
            else:
                raise DatatypeException("Unknown field size")

        elif field.generic_type == "string":
            return "VARCHAR"
        elif field.generic_type == "float":
            if field.size == 4:
                return "FLOAT"
            elif field.size == 8:
                return "DOUBLE"
            else:
                raise DatatypeException("Unknown float size")

        elif field.generic_type == "decimal":
            return "DECIMAL"
        elif field.generic_type == "datetime":
            return "DATETIME"
        elif field.generic_type == "boolean":
            return "TINYINT"
        else:
            raise DatatypeException("Unknown field type ")

    # def must_remap_field(self, field_type: FieldType) -> tuple[bool, FieldType]:
    #     if field_type == FieldType.Integer:
    #         return False, FieldType.Integer
    #     elif field_type == FieldType.String:
    #         return False, FieldType.String
    #     elif field_type == FieldType.Float:
    #         return False, FieldType.Float
    #     elif field_type == FieldType.Decimal:
    #         return False, FieldType.Decimal
    #     elif field_type == FieldType.Datetime:
    #         return False, FieldType.Datetime
    #     elif field_type == FieldType.Boolean:
    #         return True, FieldType.Integer
    #     else:
    #         raise DatatypeException("Unknown field type ")

    def replace_parameters(self, query: str) -> str:
        return re.sub(r"::(\w+)::", r"%(\1)s", query)

    def build_selection_list(self, fields:list)->str:
        return str.join(", ", [f"`{f}`" for f in fields])