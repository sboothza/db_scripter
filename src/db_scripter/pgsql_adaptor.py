import re
from typing import List

import psycopg2

from database_objects import Database, Table, KeyType, FieldType, Key, Field, DataException, DatatypeException
from adaptor import Adaptor


class PgSqlAdaptor(Adaptor):
    """ Connection string is pgsql://user:pass@hostname/database """
    __blank_connection__ = "pgsql://u:p@h/d"

    def __init__(self, connection, naming):
        super().__init__(connection, naming)

        match = re.match(r"pgsql:\/\/(\w+):(\w+)@(\w+)\/(\w+)", self.connection)
        if match:
            self.user = match.group(1)
            self.password = match.group(2)
            self.hostname = match.group(3)
            self.database = match.group(4)
        else:
            raise DataException("Invalid connection string")

    def import_schema(self, db_name: str) -> Database:
        connection = psycopg2.connect(user=self.user, password=self.password, host=self.hostname,
                                      database=self.database)

        if db_name is None:
            db_name = self.database

        database = Database(self.naming.string_to_name(db_name))
        cursor = connection.cursor()
        print("Processing tables...")
        cursor.execute("select TABLE_NAME from INFORMATION_SCHEMA.tables where TABLE_SCHEMA = 'test' and "
                       "TABLE_TYPE = 'BASE TABLE'")
        for row in cursor.fetchall():
            table = Table(self.naming.string_to_name(row[0]))
            database.tables.append(table)

        for table in database.tables:
            print(f"Processing fields for {table.name.raw()}...")
            cursor.execute("select COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, EXTRA, IS_NULLABLE, "
                           "NUMERIC_PRECISION, NUMERIC_SCALE, COLUMN_DEFAULT  from INFORMATION_SCHEMA.columns where "
                           f"TABLE_SCHEMA = '{db_name}' and TABLE_NAME='{table.name.raw()}' order by ORDINAL_POSITION")

            for row in cursor.fetchall():
                field = Field(self.naming.string_to_name(str(row[0])),
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
                           f"where fks.constraint_schema = '{db_name}' and fks.table_name = '{table.name.raw()}' "
                           "group by fks.constraint_name, fks.referenced_table_name "
                           "union "
                           "select s.INDEX_NAME as constraint_name, null as primary_table, "
                           "group_concat(s.COLUMN_NAME  order by s.SEQ_IN_INDEX separator ', ') as local_columns, "
                           "null as reference_columns, s.NON_UNIQUE, case when c.CONSTRAINT_TYPE = 'FOREIGN KEY' "
                           "then 'INDEX' when c.CONSTRAINT_TYPE is null then 'INDEX' else c.CONSTRAINT_TYPE end as "
                           "`type` from INFORMATION_SCHEMA.STATISTICS s left join "
                           "INFORMATION_SCHEMA.table_constraints c on s.TABLE_SCHEMA = c.TABLE_SCHEMA and "
                           "s.TABLE_NAME = c.TABLE_NAME and s.INDEX_NAME = c.CONSTRAINT_NAME "
                           f"where s.TABLE_SCHEMA = '{db_name}' and s.TABLE_NAME = '{table.name.raw()}' "
                           "group by s.INDEX_NAME, s.NON_UNIQUE, c.CONSTRAINT_TYPE ")
            for row in cursor.fetchall():
                key = Key(self.naming.string_to_name(row[0]))
                key.referenced_table = table.name.raw()
                key_type = row[5].lower()
                key.key_type = KeyType.get_keytype(key_type)

                key.fields = [f.strip() for f in row[2].split(",")]

                if key.key_type == KeyType.ForeignKey:
                    key.primary_table = row[1]
                    key.primary_fields = [f.strip() for f in row[3].split(",")]

                if key.key_type == KeyType.PrimaryKey:
                    table.pk = key
                else:
                    table.keys.append(key)

        connection.close()
        return database

    @staticmethod
    def get_field_size(field: Field) -> str:
        if field.type == FieldType.String:
            return f"({field.size})"
        elif field.type == FieldType.Decimal:
            return f"({field.size},{field.scale})"
        return ""

    def escape_field_list(self, values: List[str]) -> List[str]:
        return [f"\"{value}\"" for value in values]

    def generate_drop_script(self, table: Table) -> str:
        return f"DROP TABLE \"{table.name.raw()}\";"

    @staticmethod
    def get_field_default(field: Field) -> str:
        if field.type == FieldType.String or field.type == FieldType.Datetime:
            return f"'{field.default}'"
        return field.default

    def generate_create_script(self, table: Table) -> str:
        sql: list[str] = []
        for field in table.fields:
            sql.append(f"\"{field.name.raw()}\" {self.get_field_type(field.type, field.size, field.scale)}"
                       f"{self.get_field_size(field)} {'NOT NULL' if field.required else 'NULL'}"
                       f"{' GENERATED ALWAYS AS IDENTITY' if field.auto_increment else ''}"
                       f"{' DEFAULT (' + self.get_field_default(field) + ')' if field.default else ''}")
        if table.pk:
            sql.append(f"PRIMARY KEY ({','.join(table.pk.fields)})")

        for fk in [key for key in table.keys if key.key_type == KeyType.ForeignKey]:
            sql.append(f"FOREIGN KEY ({','.join(self.escape_field_list(fk.fields))}) REFERENCES "
                       f"{fk.primary_table}({','.join(self.escape_field_list(fk.primary_fields))})")
        joiner = ',\n\t'
        result = f"CREATE TABLE \"{table.name.raw()}\" (\n\t{joiner.join(sql)}\n);\n"

        for key in table.keys:
            if key.key_type == KeyType.Unique:
                result += f"CREATE UNIQUE INDEX \"{key.name.raw()}\" ON {table.name.raw()} " \
                          f"({','.join(self.escape_field_list(key.fields))});\n"

            elif key.key_type == KeyType.Index:
                result += f"CREATE INDEX \"{key.name.raw()}\" ON {table.name.raw()} " \
                          f"({','.join(self.escape_field_list(key.fields))});\n"

        return result

    def generate_table_exists_script(self, table: Table, db_name: str) -> str:
        return "select TABLE_NAME from INFORMATION_SCHEMA.tables where TABLE_SCHEMA = '{database}' and " \
               f"TABLE_TYPE = 'BASE TABLE' and TABLE_NAME = '{table.name.raw()}'"

    def generate_count_script(self, table: Table) -> str:
        return f"select count(*) from \"{table.name.raw()}\""

    def generate_insert_script(self, table: Table) -> str:
        fields = [f.name.raw() for f in table.fields if not f.auto_increment]
        params = ", ".join([f"\"{f}\"" for f in fields])
        values = ", ".join([f"%({f})s" for f in fields])
        return_id = ""
        autoinc_fields = [f for f in table.fields if f.auto_increment]
        if len(autoinc_fields) > 0:
            return_id = "returning " + ", ".join([f"{f.name.raw()}" for f in autoinc_fields])
        result = f"insert into \"{table.name.raw()}\" ({params}) values ({values}) {return_id};"

        return result

    def generate_update_script(self, table: Table) -> str:
        fields = [f.name.raw() for f in table.fields if not f.auto_increment and f.name not in table.pk.fields]
        update_list = [f"\"{f}\" = %({f})s" for f in fields]
        update = ", ".join(update_list)
        key_list = [f"\"{f}\" = %({f})s" for f in table.pk.fields]
        key = " and ".join(key_list)
        result = f"update \"{table.name.raw()}\" set {update} where {key};"
        return result

    def generate_delete_script(self, table: Table) -> str:
        key_list = [f"\"{f}\" = %({f})s" for f in table.pk.fields]
        key = " and ".join(key_list)
        result = f"delete from \"{table.name.raw()}\" where {key};"
        return result

    def generate_fetch_by_id_script(self, table: Table) -> str:
        field_list = [f"\"{f.name.raw()}\"" for f in table.fields]
        fields = ", ".join(field_list)
        key_list = [f"\"{f}\" = %({f})s" for f in table.pk.fields]
        key = " and ".join(key_list)
        result = f"select {fields} from \"{table.name.raw()}\" where {key};"
        return result

    def generate_item_exists_script(self, table: Table) -> str:
        key_list = [f"\"{f}\" = %({f})s" for f in table.pk.fields]
        key = " and ".join(key_list)
        result = f"select count(*) from \"{table.name.raw()}\" where {key};"
        return result

    @staticmethod
    def get_field_type_defaults(value: str, field: Field, size, precision, scale, default):
        value = value.lower()
        default = None if default is None else default.decode("utf-8")
        if value == "integer" or value == "int":
            field.type = FieldType.Integer
            field.size = 4
        elif value == "bigint":
            field.type = FieldType.Integer
            field.size = 8
        elif value == "smallint":
            field.type = FieldType.Integer
            field.size = 2
        elif value == "float" or value == "real":
            field.type = FieldType.Float
            field.size = 4
        elif value == "double":
            field.type = FieldType.Float
            field.size = 8
        elif value == "boolean" or value == "bool":
            field.type = FieldType.Boolean
            field.size = 1
        elif value == "decimal" or value == "money" or value == "numeric":
            field.type = FieldType.Decimal
            field.size = precision
            field.scale = scale
        elif value == "string" or value == "varchar" or value == "char":
            field.type = FieldType.String
            field.size = size
        elif value == "timestamp" or value == "date":
            field.type = FieldType.Datetime
            field.size = 0
        elif value == "none" or value == "undefined":
            field.type = FieldType.Undefined
            field.size = 0
        else:
            raise DatatypeException("Unknown field type {}".format(value))
        field.default = default

    def get_field_type(self, field_type: FieldType, size: int = 0, scale: int = 0) -> str:
        if field_type == FieldType.Integer:
            if size == 1 or size == 2:
                return "SMALLINT"
            elif size == 4:
                return "INT"
            elif size == 8:
                return "BIGINT"
            else:
                raise DatatypeException("Unknown field size")

        elif field_type == FieldType.String:
            return "VARCHAR"
        elif field_type == FieldType.Float:
            if size == 4:
                return "FLOAT"
            elif size == 8:
                return "DOUBLE"
            else:
                raise DatatypeException("Unknown float size")

        elif field_type == FieldType.Decimal:
            return "NUMERIC"
        elif field_type == FieldType.Datetime:
            return "TIMESTAMP"
        elif field_type == FieldType.Boolean:
            return "SMALLINT"
        else:
            raise DatatypeException("Unknown field type ")

    def must_remap_field(self, field_type: FieldType) -> tuple[bool, FieldType]:
        if field_type == FieldType.Integer:
            return False, FieldType.Integer
        elif field_type == FieldType.String:
            return False, FieldType.String
        elif field_type == FieldType.Float:
            return False, FieldType.Float
        elif field_type == FieldType.Decimal:
            return False, FieldType.Decimal
        elif field_type == FieldType.Datetime:
            return False, FieldType.Datetime
        elif field_type == FieldType.Boolean:
            return True, FieldType.Integer
        else:
            raise DatatypeException("Unknown field type ")

    def replace_parameters(self, query: str) -> str:
        return re.sub(r"::(\w+)::", r"%(\1)s", query)

    def build_selection_list(self, fields: list) -> str:
        return str.join(", ", [f"\"{f}\"" for f in fields])
