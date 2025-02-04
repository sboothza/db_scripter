import re
from typing import List, Dict

import pymssql
from pymssql import Connection

from adaptor import Adaptor
from database_objects import Database, Table, KeyType, FieldType, Key, Field, DataException, DatatypeException, View, \
    UDDT, UDTT


class MsSqlAdaptor(Adaptor):
    """ Connection string is mssql://user:pass@hostname/database """
    __blank_connection__ = "mssql://u:p@h/d"
    options: dict[str, str]

    def __init__(self, connection, naming):
        super().__init__(connection, naming)
        self.options = {}

        match = re.match(r"mssql:\/\/((\w*):(\w*)@)?([^\/]+)\/([^\?]+)(\?.+)?", self.connection)
        if match:
            self.user = match.group(2)
            self.password = match.group(3)
            self.hostname = match.group(4)
            self.database = match.group(5)
            options = match.group(6)
            options_match = re.findall(r"(\w+)=(\w+)", options)
            for option_match in options_match:
                self.options[option_match[0]] = option_match[1]

        else:
            raise DataException("Invalid connection string")

    def get_option(self, name: str, default: str) -> str:
        if name not in self.options.keys():
            return default
        return self.options[name]

    def connect(self) -> Connection:
        connection = None
        if self.get_option("integrated_authentication", "False") == "True":
            connection = pymssql.connect(host=self.hostname, database=self.database)
        else:
            connection = pymssql.connect(user=self.user, password=self.password, host=self.hostname,
                                   database=self.database)

        cursor = connection.cursor(as_dict=True)
        cursor.execute("select GETDATE() as d;")
        return connection

    def import_schema(self, db_name: str) -> Database:
        connection = self.connect()

        if db_name is None:
            db_name = self.database

        database = Database(db_name)
        cursor = connection.cursor(as_dict=True)
        print("Processing tables...")
        cursor.execute(
            "select schema_name(tab.schema_id) as schema_name, tab.name as table_name, col.column_id as id, col.name, "
            "t.name as data_type, col.max_length, col.precision, col.is_nullable, "
            "COLUMNPROPERTY(object_id(schema_name(tab.schema_id)+'.'+tab.name), col.name, 'IsIdentity') as IS_IDENTITY, d.definition as default_value "
            "from sys.tables as tab "
            "inner join sys.columns as col on tab.object_id = col.object_id "
            "left join sys.types as t on col.user_type_id = t.user_type_id "
            "left join sys.default_constraints d on d.object_id = col.default_object_id "
            "order by tab.name, column_id;")
        table_name = "none"
        table = None
        for row in cursor.fetchall():
            new_table_name = f"{row["schema_name"]}.{row["table_name"]}"
            if table_name != new_table_name:
                print(new_table_name)
                table_name = new_table_name
                table = Table(row["table_name"], row["schema_name"])
                database.tables.append(table)

            field = Field(str(row["name"]),
                          auto_increment=row["IS_IDENTITY"] == 1,
                          required=row["is_nullable"])
            self.get_field_type_defaults(row["data_type"], field, row["max_length"], row["precision"],
                                         row["precision"], row["default_value"])

            table.fields.append(field)

        print("Processing views...")
        cursor.execute("select schema_name(v.schema_id) as schema_name, v.name as view_name, c.name, c.column_id, "
                       "t.name as data_type, c.max_length, c.precision, c.is_nullable, m.definition "
                       "from sys.views v "
                       "inner join sys.sql_modules m on m.object_id = v.object_id "
                       "inner join sys.columns c on c.object_id = v.object_id "
                       "left join sys.types as t on c.user_type_id = t.user_type_id "
                       "order by schema_name, view_name, c.column_id")
        view_name = "none"
        view = None
        for row in cursor.fetchall():
            new_view_name = f"{row["schema_name"]}.{row["view_name"]}"
            if view_name != new_view_name:
                print(new_view_name)
                view_name = new_view_name
                view = View(row["view_name"], row["schema_name"])
                view.definition = row["definition"]
                database.tables.append(view)

            field = Field(str(row["name"]),
                          required=row["is_nullable"])
            self.get_field_type_defaults(row["data_type"], field, row["max_length"], row["precision"],
                                         row["precision"], None)

            view.fields.append(field)

        print("Processing uddts...")
        cursor.execute(
            "select schema_name(t.schema_id) as schema_name, t.name, tp.name as base_type, t.max_length, "
            "t.precision, t.scale, t.is_nullable "
            "from sys.types t "
            "inner join sys.types tp on tp.system_type_id = t.system_type_id "
            "where t.is_user_defined = 1 and t.is_table_type = 0")

        uddt_name = "none"
        uddt = None
        for row in cursor.fetchall():
            new_uddt_name = f"{row["schema_name"]}.{row["name"]}"
            if uddt_name != new_uddt_name:
                print(new_uddt_name)
                udt_name = new_uddt_name
                udt = UDDT(row["name"], row["schema_name"], required=row["is_nullable"] == 0)
                self.get_field_type_defaults(row["base_type"], udt, row["max_length"], row["precision"],
                                             row["scale"], None)
                database.uddts.append(uddt)

        print("Processing udtts...")
        cursor.execute(
            "SELECT SCHEMA_NAME(TYPE.schema_id) as schema_name, TYPE.name AS \"Type Name\", COL.column_id, "
            "COL.name AS \"Column\", ST.name AS \"Data Type\", "
            "CASE COL.Is_Nullable "
            "WHEN 1 THEN 1 "
            "ELSE 0 "
            "END AS \"Nullable\", COL.max_length AS \"Length\", COL.[precision] AS \"Precision\", COL.scale AS \"Scale\" "
            "FROM sys.table_types TYPE "
            "JOIN sys.columns COL ON TYPE.type_table_object_id = COL.object_id "
            "JOIN sys.systypes AS ST ON ST.xtype = COL.system_type_id "
            "where TYPE.is_user_defined = 1 "
            "ORDER BY schema_name, \"Type Name\", COL.column_id")

        udtt_name = "none"
        udtt = None
        for row in cursor.fetchall():
            new_udtt_name = f"{row["schema_name"]}.{row["Type Name"]}"
            if udtt_name != new_udtt_name:
                print(new_udtt_name)
                udtt_name = new_udtt_name
                udtt = UDTT(row["Type Name"], row["schema_name"])
                database.udtts.append(udtt)

            field = Field(row["Column"], required=row["Nullable"] == 0)
            self.get_field_type_defaults(row["Data Type"], field, row["Length"], row["Precision"],
                                         row["Scale"], None)

            udtt.fields.append(field)

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
    def get_field_type_defaults(value: str, field, size, precision, scale, default):
        value = value.lower()
        default = None if default is None else str(default)
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
        elif value == "boolean" or value == "bool" or value == "bit":
            field.type = FieldType.Boolean
            field.size = 1
        elif value == "decimal" or value == "money" or value == "numeric":
            field.type = FieldType.Decimal
            field.size = precision
            field.scale = scale
        elif value == "string" or value == "varchar" or value == "char" or value == "nchar" or value == "nvarchar":
            field.type = FieldType.String
            field.size = size
        elif value == "varbinary" or value == "text" or value == "xml":
            field.type = FieldType.Binary
            field.size = size
        elif value == "datetime" or value == "date" or value == "datetime2" or value == "smalldatetime" or value == "timestamp":
            field.type = FieldType.Datetime
            field.size = 0
        elif value == "none" or value == "undefined":
            field.type = FieldType.Undefined
            field.size = 0
        elif value == "uniqueidentifier":
            field.type = FieldType.UniqueIdentifier
            field.size = 0
        elif value == "sysname":
            field.type = FieldType.String
            field.size = 128
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
