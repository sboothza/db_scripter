import os.path
import re
from typing import List

import pymssql
from pymssql import Connection
from sb_serializer import Name
from toposort import toposort_flatten

from adaptor import Adaptor
from database_objects import Database, Table, KeyType, FieldType, Field, DataException, DatatypeException, View, \
    UDDT, UDTT, StoredProcedure, FunctionType, QualifiedName
from src.db_scripter.common import create_dir
from src.db_scripter.database_objects import Function


class MsSqlAdaptor(Adaptor):
    """ Connection string is mssql://user:pass@hostname/database """
    __blank_connection__ = "mssql://u:p@h/d"
    options: dict[str, str]

    def __init__(self, connection, naming):
        super().__init__(connection, naming)
        self.options = {}

        match = re.match(r"mssql://((\w*):(\w*)@)?([^/]+)/([^?]+)(\?.+)?", self.connection)
        if match:
            self.user = match.group(2)
            self.password = match.group(3)
            self.hostname = match.group(4)
            self.database = match.group(5)
            options = match.group(6)
            if options is not None:
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

        database = Database(Name(db_name))
        cursor = connection.cursor(as_dict=True)

        print("Processing uddts...")
        cursor.execute(
            "select schema_name(t.schema_id) as schema_name, t.name, tp.name as base_type, t.max_length, "
            "t.precision, t.scale, t.is_nullable "
            "from sys.types t "
            "inner join sys.types tp on tp.is_user_defined = 0 and tp.system_type_id = t.system_type_id and "
            "tp.user_type_id = tp.system_type_id "
            "where t.is_user_defined = 1 and t.is_table_type = 0")

        for row in cursor.fetchall():
            print(f"{row["schema_name"]}.{row["name"]}")
            udt = UDDT(name=QualifiedName(row["schema_name"], row["name"]), required=row["is_nullable"] == 0,
                       native_type=row["base_type"])
            self.get_field_type_defaults(database, row["base_type"], udt, row["max_length"], row["precision"],
                                         row["scale"], None)
            database.uddts.append(udt)

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
                table = Table(QualifiedName(row["schema_name"], row["table_name"]))
                database.tables.append(table)

            field = Field(row["name"],
                          auto_increment=row["IS_IDENTITY"] == 1,
                          required=row["is_nullable"], native_type=row["data_type"])
            self.get_field_type_defaults(database, row["data_type"], field, row["max_length"], row["precision"],
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
                view = View(QualifiedName(row["schema_name"], row["view_name"]))
                view.definition = row["definition"]
                database.tables.append(view)

            field = Field(Name(row["name"]), required=row["is_nullable"], native_type=row["data_type"])
            self.get_field_type_defaults(database, row["data_type"], field, row["max_length"], row["precision"],
                                         row["precision"], None)

            view.fields.append(field)

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
                udtt = UDTT(QualifiedName(row["schema_name"], row["Type Name"]))
                database.udtts.append(udtt)

            field = Field(row["Column"], required=row["Nullable"] == 0, native_type=row["data_type"])
            # is uddt?
            t = database.get_type(row["Data Type"])
            if t is not None:
                field.generic_type = t
            else:
                self.get_field_type_defaults(database, row["Data Type"], field, row["Length"], row["Precision"],
                                             row["Scale"], None)

            udtt.fields.append(field)

        print("Processing functions...")
        cursor.execute(
            "SELECT schema_name(o.schema_id) as schema_name, o.name, m.definition, "
            "case o.type_desc "
            "when 'SQL_SCALAR_FUNCTION' THEN 'function' "
            "when 'SQL_TABLE_VALUED_FUNCTION' THEN 'table function' "
            "when 'SQL_INLINE_TABLE_VALUED_FUNCTION' THEN 'table function' "
            "end as type "
            "FROM sys.sql_modules m "
            "INNER JOIN sys.objects o "
            "ON m.object_id=o.object_id "
            "WHERE o.type_desc like '%function%'")

        for row in cursor.fetchall():
            print(f"{row["schema_name"]}.{row["name"]}")
            f = Function(QualifiedName(row["schema_name"], row["name"]), row["definition"],
                         FunctionType.from_str(row["type"]))
            database.functions.append(f)

        print("Processing stored procedures...")
        cursor.execute(
            "select schema_name(schema_id) as schema_name, name, object_definition(object_id) as text from sys.procedures")

        for row in cursor.fetchall():
            print(f"{row["schema_name"]}.{row["name"]}")
            sp = StoredProcedure(QualifiedName(row["schema_name"], row["name"]), row["text"])
            database.stored_procedures.append(sp)

        # print("Processing dependencies...")
        # cursor.execute(
        #     "select distinct OBJECT_SCHEMA_NAME (o.id) as schema_name, o.name, o.xtype, "
        #     "OBJECT_SCHEMA_NAME (ref.id) as ref_schema_name, ref.name as referenced, ref.xtype as reftype "
        #     "from sys.sql_dependencies d "
        #     "inner join sys.sysobjects o on o.id = d.object_id "
        #     "inner join sys.sysobjects ref on ref.id = d.referenced_major_id")
        #
        # for row in cursor.fetchall():
        #     print(f"{row["schema_name"]}.{row["name"]} => {row["ref_schema_name"]}.{row["referenced"]}")
        #     obj = database.get_object(QualifiedName(row["schema_name"], row["name"]),
        #                               self.get_object_type(row["xtype"]))
        #     if obj is None:
        #         raise DataException("Couldn't find object!")
        #     ref = database.get_object(QualifiedName(row["ref_schema_name"], row["referenced"]),
        #                               self.get_object_type(row["reftype"]))
        #     if ref is None:
        #         raise DataException("Couldn't find object!")
        #     dep = Dependancy(obj.name, ref.name, self.get_object_type(row["reftype"]))
        #     database.dependancies.append(dep)
        #
        # print("Processing udtt dependencies...")
        # cursor.execute(
        #     "Select distinct SPECIFIC_SCHEMA, SPECIFIC_NAME, USER_DEFINED_TYPE_SCHEMA, USER_DEFINED_TYPE_NAME "
        #     "From Information_Schema.PARAMETERS "
        #     "Where USER_DEFINED_TYPE_NAME is not null "
        #     "order by SPECIFIC_SCHEMA, SPECIFIC_NAME")
        #
        # for row in cursor.fetchall():
        #     print(
        #         f"{row["SPECIFIC_SCHEMA"]}.{row["SPECIFIC_NAME"]} => {row["USER_DEFINED_TYPE_SCHEMA"]}.{row["USER_DEFINED_TYPE_NAME"]}")
        #     obj = database.get_object(QualifiedName(row["SPECIFIC_SCHEMA"], row["SPECIFIC_NAME"]), "StoredProcedure")
        #     if obj is None:
        #         raise DataException("Couldn't find object!")
        #     ref = database.get_object(QualifiedName(row["USER_DEFINED_TYPE_SCHEMA"], row["USER_DEFINED_TYPE_NAME"]),
        #                               self.get_object_type("UDTT"))
        #     if ref is None:
        #         raise DataException("Couldn't find object!")
        #     dep = Dependancy(obj.name, ref.name, "UDTT")
        #     database.dependancies.append(dep)

        connection.close()
        return database

    def calculate_sp_dependencies(self, database: Database) -> List[StoredProcedure]:
        dependencies = {d.obj: [] for d in database.dependancies}
        for item in database.dependancies:
            dependencies[item.obj].append(item.referenced_obj)

        graph = dict(zip(dependencies.keys(), map(set, dependencies.values())))
        sorted_graph = toposort_flatten(graph, sort=True)

        remaining_list = [item.name for item in database.stored_procedures if item.name not in sorted_graph]

        sorted_graph.extend(remaining_list)

        sorted_sps = [database.get_stored_procedure(sp_name) for sp_name in sorted_graph]

        return sorted_sps

    def write_schema(self, database: Database, path: str):
        # write tables
        print("Writing table scripts....")
        local_path = os.path.join(path, "tables")

        create_dir(local_path, delete=True)

        counter = 1
        for table in database.tables:
            with open(os.path.join(local_path, f"{counter:03}-{table.name.name}.sql"), "w", 1024, encoding="utf8") as f:
                f.write(self.generate_create_script(table))
                f.flush()
            counter += 1

        print("Writing drop sp scripts....")
        local_path = os.path.join(path, "sp")
        create_dir(local_path, de)

        # calculating dependencies
        stored_procs = self.calculate_sp_dependencies(database)

        with open(os.path.join(local_path, "drop_sp.sql"), "w", 1024, encoding="utf8") as f:
            for sp in stored_procs:
                sql = (
                    f"IF EXISTS ( SELECT * FROM sysobjects WHERE id = object_id(N'{sp.name.schema}.{sp.name.name}') and "
                    f"OBJECTPROPERTY(id, N'IsProcedure') = 1 )\nBEGIN\n\tDROP PROCEDURE {sp.name.schema}.{sp.name.name}\nEND\n\n")
                f.write(sql)

        print("Writing drop udt scripts....")
        local_path = os.path.join(path, "udt")
        create_dir(local_path)
        with open(os.path.join(local_path, "drop_udt.sql"), "w", 1024, encoding="utf8") as f:
            for udt in database.uddts:
                sql = (
                    f"IF EXISTS ( SELECT * FROM sysobjects WHERE id = object_id(N'{udt.name.schema}.{udt.name.name}')\n\n"
                    f"BEGIN\n\tDROP TYPE {udt.name.schema}.{udt.name.name}\nEND\n")
                f.write(sql)

        print("Writing drop udtt scripts....")
        local_path = os.path.join(path, "udtt")
        create_dir(local_path)
        with open(os.path.join(local_path, "drop_udtt.sql"), "w", 1024, encoding="utf8") as f:
            for udtt in database.udtts:
                sql = (
                    f"IF EXISTS ( SELECT * FROM sysobjects WHERE id = object_id(N'{udtt.name.schema}.{udtt.name.name}')\n\n"
                    f"BEGIN\n\tDROP TYPE {udtt.name.schema}.{udtt.name.name}\nEND\n")
                f.write(sql)

        print("Writing UDT scripts....")
        local_path = os.path.join(path, "udt")
        create_dir(local_path)
        for udt in database.uddts:
            with open(os.path.join(local_path, udt.name.name + ".sql"), "w", 1024, encoding="utf8") as f:
                f.write(self.generate_create_uddt_script(udt))
                f.flush()

        print("Writing UDTT scripts....")
        local_path = os.path.join(path, "udtt")
        create_dir(local_path)
        for udt in database.udtts:
            with open(os.path.join(local_path, udt.name.name + ".sql"), "w", 1024, encoding="utf8") as f:
                f.write(self.generate_create_udtt_script(udt))
                f.flush()

        print("Writing SP scripts....")
        local_path = os.path.join(path, "sp")
        create_dir(local_path)
        counter = 1
        for sp in stored_procs:
            with open(os.path.join(local_path, f"{counter:03}-{sp.name.name}.sql"), "w", 1024, encoding="utf8") as f:
                f.write(self.generate_create_sp_script(sp))
                f.flush()
            counter += 1

    @staticmethod
    def get_object_type(name: str) -> str | None:
        name = name.strip().upper()
        if name == "P":
            return "StoredProcedure"
        elif name == "FN" or name == "TF" or name == "IF":
            return "Function"
        elif name == "UDTT":
            return "UDTT"
        elif name == "U":
            return "Table"
        elif name == "V":
            return "View"
        else:
            raise DataException("Couldn't find type!")

    @staticmethod
    def get_field_size(field: Field | UDDT) -> str:
        if field.generic_type == FieldType.String:
            return f"({field.size})"
        elif field.generic_type == FieldType.Decimal:
            return f"({field.size},{field.scale})"
        return ""

    @staticmethod
    def get_field_default(field: Field) -> str:
        if field.generic_type == FieldType.String or field.generic_type == FieldType.Datetime:
            return f"'{field.default}'"
        return field.default

    def generate_create_script(self, table: Table) -> str:
        sql: list[str] = []
        for field in table.fields:
            field_sql = (f"[{field.name}] {self.get_field_type(field)}"
                         f"{self.get_field_size(field)} {'NOT NULL' if field.required else 'NULL'}"
                         f"{' IDENTITY(1, 1)' if field.auto_increment else ''}"
                         f"{' DEFAULT (' + self.get_field_default(field) + ')' if field.default else ''}")
            sql.append(field_sql)

        if table.pk:
            sql.append(f"PRIMARY KEY ({','.join(table.pk.fields)})")

        for fk in [key for key in table.keys if key.key_type == KeyType.ForeignKey]:
            sql.append(f"FOREIGN KEY ({','.join(self.escape_field_list(fk.fields))}) REFERENCES "
                       f"{fk.primary_table}({','.join(self.escape_field_list(fk.primary_fields))})")
        joiner = ',\n\t'
        result = f"CREATE TABLE [{table.name}] (\n\t{joiner.join(sql)}\n);\n"

        for key in table.keys:
            if key.key_type == KeyType.Unique:
                result += f"CREATE UNIQUE INDEX [{key.name}] ON {table.name} " \
                          f"({','.join(self.escape_field_list(key.fields))});\n"

            elif key.key_type == KeyType.Index:
                result += f"CREATE INDEX [{key.name}] ON {table.name} " \
                          f"({','.join(self.escape_field_list(key.fields))});\n"

        return result

    def generate_create_sp_script(self, sp: StoredProcedure) -> str:
        sql = sp.text
        return sql

    def generate_create_uddt_script(self, udt: UDDT) -> str:
        sql = f"CREATE TYPE {udt.name.schema}.{udt.name.name} FROM "
        f"[{udt.generic_type}] {self.get_field_type(udt)}"
        f"{self.get_field_size(udt)} {'NOT NULL' if udt.required else 'NULL'}"
        return sql

    def generate_create_udtt_script(self, udt: UDTT) -> str:
        sql = f"CREATE TYPE [{udt.name.schema}].[{udt.name.name}] AS TABLE ("

        field_sql = [f"[{field.name.name}] {self.get_field_type(field)} "
                     f"{self.get_field_size(field)} {'NOT NULL' if field.required else 'NULL'}"
                     f"{' IDENTITY(1, 1)' if field.auto_increment else ''} " for field in udt.fields]

        sql += ".".join(field_sql)
        sql += ");"
        return sql

    @staticmethod
    def get_field_type_defaults(database: Database, value: str, field: Field | UDDT, size, precision, scale, default):
        value = value.lower()

        default = None if default is None else str(default)
        if value == "integer" or value == "int":
            field.generic_type = FieldType.Integer
            field.size = 4
        elif value == "bigint":
            field.generic_type = FieldType.Integer
            field.size = 8
        elif value == "tinyint":
            field.generic_type = FieldType.Integer
            field.size = 1
        elif value == "smallint":
            field.generic_type = FieldType.Integer
            field.size = 2
        elif value == "mediumint":
            field.generic_type = FieldType.Integer
            field.size = 3
        elif value == "float" or value == "real":
            field.generic_type = FieldType.Float
            field.size = 4
        elif value == "double":
            field.generic_type = FieldType.Float
            field.size = 8
        elif value == "boolean" or value == "bool" or value == "bit":
            field.generic_type = FieldType.Boolean
            field.size = 1
        elif value == "decimal" or value == "money" or value == "numeric" or value == "smallmoney":
            field.generic_type = FieldType.Decimal
            field.size = precision
            field.scale = scale
        elif value == "string" or value == "varchar" or value == "nvarchar" or value == "char" or value == "nchar":
            field.generic_type = FieldType.String
            field.size = size
        elif value == "varbinary" or value == "text" or value == "xml":
            field.generic_type = FieldType.Binary
            field.size = size
        elif value == "datetime" or value == "date" or value == "datetime2" or value == "smalldatetime" or value == "timestamp" or value == "time":
            field.generic_type = FieldType.Datetime
            field.size = 0
        elif value == "none" or value == "undefined":
            field.generic_type = FieldType.Undefined
            field.size = 0
        elif value == "uniqueidentifier":
            field.generic_type = FieldType.UniqueIdentifier
            field.size = 0
        elif value == "sysname":
            field.generic_type = FieldType.String
            field.size = 128
        elif value == "hierarchyid":
            field.generic_type = FieldType.Hierarchy
            field.size = 1
        else:
            uddt = database.get_type(value)
            if uddt is None:
                raise DatatypeException("Unknown field type {}".format(value))
            else:
                field.generic_type = uddt

        field.default = default

    def get_field_type(self, field: Field | UDDT) -> str:
        if field.native_type is not None:
            return field.native_type

        if field.generic_type == FieldType.Integer:
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
        elif field.generic_type == FieldType.String:
            return "VARCHAR"
        elif field.generic_type == FieldType.Float:
            if field.size == 4:
                return "FLOAT"
            elif field.size == 8:
                return "DOUBLE"
            else:
                raise DatatypeException("Unknown float size")

        elif field.generic_type == FieldType.Decimal:
            return "DECIMAL"
        elif field.generic_type == FieldType.Datetime:
            return "DATETIME"
        elif field.generic_type == FieldType.Boolean:
            return "TINYINT"
        elif field.generic_type == FieldType.UniqueIdentifier:
            return "UNIQUEIDENTIFIER"
        else:
            raise DatatypeException("Unknown field type ")

    def escape_field_list(self, values: List[str]) -> List[str]:
        return [f"`{value}`" for value in values]
