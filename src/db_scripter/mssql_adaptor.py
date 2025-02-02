import re
from typing import List

import pymssql

from adaptor import Adaptor
from database_objects import Database, Table, KeyType, FieldType, Key, Field, DataException, DatatypeException


class MsSqlAdaptor(Adaptor):
    """ Connection string is mssql://user:pass@hostname/database """
    __blank_connection__ = "mssql://u:p@h/d"

    def __init__(self, connection, naming):
        super().__init__(connection, naming)

        match = re.match(r"mssql:\/\/(\w+):(\w+)@(\w+)\/(\w+)", self.connection)
        if match:
            self.user = match.group(1)
            self.password = match.group(2)
            self.hostname = match.group(3)
            self.database = match.group(4)
        else:
            raise DataException("Invalid connection string")

    def import_schema(self, db_name: str) -> Database:
        connection = pymssql.connect(user=self.user, password=self.password, host=self.hostname,
                                     database=self.database)

        if db_name is None:
            db_name = self.database

        database = Database(self.naming.string_to_name(db_name))
        cursor = connection.cursor(buffered=True)
        print("Processing tables...")
        cursor.execute(
            "select schema_name(tab.schema_id) as schema_name, tab.name as table_name, col.column_id as id, col.name, "
            "t.name as data_type, col.max_length, col.precision, col.is_nullable, "
            "COLUMNPROPERTY(object_id(schema_name(tab.schema_id)+'.'+tab.name), col.name, 'IsIdentity') as IS_IDENTITY "
            "from sys.tables as tab "
            "inner join sys.columns as col on tab.object_id = col.object_id "
            "left join sys.types as t on col.user_type_id = t.user_type_id "
            "order by tab.name, column_id;")
        table_name = "none"
        table = None
        for row in cursor.fetchall():
            new_table_name = f"{row[0]}.{row[1]}"
            if table_name != new_table_name:
                table_name = new_table_name
                table = Table(self.naming.string_to_name(row[1]), self.naming.string_to_name(row[0]))
                database.tables.append(table)

            field = Field(self.naming.string_to_name(str(row[1])),
                          auto_increment=row[8] == 1,
                          required=row[7] == 1)
            self.get_field_type_defaults(row[4].decode("utf-8"), field, row[5], row[6],
                                         row[5], row[9])

            print("Processing indexes and keys")

            cursor.execute("select schema_name(t.schema_id) + '.' + t.[name] as table_view, "
                           "case when t.[type] = 'U' then 'Table' "
                           "when t.[type] = 'V' then 'View' "
                           "end as [object_type], "
                           "case when c.[type] = 'PK' then 'Primary key' "
                           "when c.[type] = 'UQ' then 'Unique constraint' "
                           "when i.[type] = 1 then 'Unique clustered index' "
                           "when i.type = 2 then 'Unique index' "
                           "end as constraint_type, "
                           "c.[name] as constraint_name, "
                           "substring(column_names, 1, len(column_names)-1) as [columns], "
                           "i.[name] as index_name, "
                           "case when i.[type] = 1 then 'Clustered index' "
                           "when i.type = 2 then 'Index' "
                           "end as index_type "
                           "from sys.objects t "
                           "left outer join sys.indexes i "
                           "on t.object_id = i.object_id "
                           "left outer join sys.key_constraints c "
                           "on i.object_id = c.parent_object_id "
                           "and i.index_id = c.unique_index_id "
                           "cross apply (select col.[name] + ', ' "
                           "from sys.index_columns ic "
                           "inner join sys.columns col "
                           "on ic.object_id = col.object_id "
                           "and ic.column_id = col.column_id "
                           "where ic.object_id = t.object_id "
                           "and ic.index_id = i.index_id "
                           "order by col.column_id "
                           "for xml path ('') ) D (column_names) "
                           "where is_unique = 1 "
                           "and t.is_ms_shipped <> 1 "
                           "order by schema_name(t.schema_id) + '.' + t.[name] ")

            for row in cursor.fetchall():
                key = Key(self.naming.string_to_name(row[3]))
                key.referenced_table = table.name.raw()
                key_type = row[2].lower()
                key.key_type = KeyType.get_keytype(key_type)

                key.fields = [f.strip() for f in row[4].split(",")]

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
        return [f"`{value}`" for value in values]

    def generate_drop_script(self, table: Table) -> str:
        return f"DROP TABLE `{table.name.raw()}`;"

    @staticmethod
    def get_field_default(field: Field) -> str:
        if field.type == FieldType.String or field.type == FieldType.Datetime:
            return f"'{field.default}'"

    def generate_create_script(self, table: Table) -> str:
        sql: list[str] = []
        for field in table.fields:
            sql.append(f"`{field.name.raw()}` {self.get_field_type(field.type, field.size, field.scale)}"
                       f"{self.get_field_size(field)} {'NOT NULL' if field.required else 'NULL'}"
                       f"{' AUTO_INCREMENT' if field.auto_increment else ''}"
                       f"{' DEFAULT (' + self.get_field_default(field) + ')' if field.default else ''}")
        if table.pk:
            sql.append(f"PRIMARY KEY ({','.join(table.pk.fields)})")

        for fk in [key for key in table.keys if key.key_type == KeyType.ForeignKey]:
            sql.append(f"FOREIGN KEY ({','.join(self.escape_field_list(fk.fields))}) REFERENCES "
                       f"{fk.primary_table}({','.join(self.escape_field_list(fk.primary_fields))})")
        joiner = ',\n\t'
        result = f"CREATE TABLE `{table.name.raw()}` (\n\t{joiner.join(sql)}\n);\n"

        for key in table.keys:
            if key.key_type == KeyType.Unique:
                result += f"CREATE UNIQUE INDEX `{key.name.raw()}` ON {table.name.raw()} " \
                          f"({','.join(self.escape_field_list(key.fields))});\n"

            elif key.key_type == KeyType.Index:
                result += f"CREATE INDEX `{key.name.raw()}` ON {table.name.raw()} " \
                          f"({','.join(self.escape_field_list(key.fields))});\n"

        return result

    def generate_table_exists_script(self, table: Table, db_name: str) -> str:
        return f"select TABLE_NAME from INFORMATION_SCHEMA.tables where TABLE_SCHEMA = '{db_name}' and " \
               f"TABLE_TYPE = 'BASE TABLE' and TABLE_NAME = '{table.name.raw()}'"

    def generate_count_script(self, table: Table) -> str:
        return f"select count(*) from `{table.name.raw()}`"

    def generate_insert_script(self, table: Table) -> str:
        fields = [f.name.raw() for f in table.fields if not f.auto_increment]
        params = ", ".join([f"`{f}`" for f in fields])
        values = ", ".join([f"%({f})s" for f in fields])
        result = f"insert into `{table.name.raw()}` ({params}) values ({values});"
        return result

    def generate_update_script(self, table: Table) -> str:
        fields = [f.name.raw() for f in table.fields if not f.auto_increment and f.name not in table.pk.fields]
        update_list = [f"`{f}` = %({f})s" for f in fields]
        update = ", ".join(update_list)
        key_list = [f"`{f}` = %({f})s" for f in table.pk.fields]
        key = " and ".join(key_list)
        result = f"update `{table.name.raw()}` set {update} where {key};"
        return result

    def generate_delete_script(self, table: Table) -> str:
        key_list = [f"`{f}` = %({f})s" for f in table.pk.fields]
        key = " and ".join(key_list)
        result = f"delete from `{table.name.raw()}` where {key};"
        return result

    def generate_fetch_by_id_script(self, table: Table) -> str:
        field_list = [f"`{f.name.raw()}`" for f in table.fields]
        fields = ", ".join(field_list)
        key_list = [f"`{f}` = %({f})s" for f in table.pk.fields]
        key = " and ".join(key_list)
        result = f"select {fields} from `{table.name.raw()}` where {key};"
        return result

    def generate_item_exists_script(self, table: Table) -> str:
        key_list = [f"`{f}` = %({f})s" for f in table.pk.fields]
        key = " and ".join(key_list)
        result = f"select count(*) from `{table.name.raw()}` where {key};"
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
        elif value == "tinyint":
            field.type = FieldType.Integer
            field.size = 1
        elif value == "smallint":
            field.type = FieldType.Integer
            field.size = 2
        elif value == "mediumint":
            field.type = FieldType.Integer
            field.size = 3
        elif value == "float" or value == "real":
            field.type = FieldType.Float
            field.size = 4
        elif value == "double":
            field.type = FieldType.Float
            field.size = 8
        elif value == "boolean" or value == "bool":
            field.type = FieldType.Boolean
            field.size = 1
        elif value == "decimal" or value == "money":
            field.type = FieldType.Decimal
            field.size = precision
            field.scale = scale
        elif value == "string" or value == "varchar" or value == "char":
            field.type = FieldType.String
            field.size = size
        elif value == "datetime" or value == "date":
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
            if size == 1:
                return "TINYINT"
            elif size == 2:
                return "SMALLINT"
            elif size == 3:
                return "MEDIUMINT"
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
            return "DECIMAL"
        elif field_type == FieldType.Datetime:
            return "DATETIME"
        elif field_type == FieldType.Boolean:
            return "TINYINT"
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
        return str.join(", ", [f"`{f}`" for f in fields])
