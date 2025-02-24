import os.path
import re
from typing import List

import pymssql
from pymssql import Connection
from toposort import toposort_flatten

from adaptor import Adaptor
from common import create_dir, naming
from database_objects import Database, Table, KeyType, Field, DataException, DatatypeException, View, \
    UDDT, UDTT, StoredProcedure, FunctionType, QualifiedName, Dependancy, Key, Constraint
from database_objects import Function
from options import Options


class MsSqlAdaptor(Adaptor):
    """ Connection string is mssql://user:pass@hostname/database """
    __blank_connection__ = "mssql://u:p@h/d"
    options: Options

    def __init__(self, connection):
        super().__init__(connection)

        match = re.match(r"mssql://((\w*):(\w*)@)?([^/]+)/([^?]+)(\?.+)?", self.connection)
        if match:
            self.user = match.group(2)
            self.password = match.group(3)
            self.hostname = match.group(4)
            self.database = match.group(5)
            options = match.group(6)
            if options is not None:
                self.options = Options(options)
            else:
                self.options = Options()

        else:
            raise DataException("Invalid connection string")

    def connect(self) -> Connection:
        connection = None
        if self.options["integrated_authentication", "False"] == "True":
            connection = pymssql.connect(host=self.hostname, database=self.database)
        else:
            connection = pymssql.connect(user=self.user, password=self.password, host=self.hostname,
                                         database=self.database)

        cursor = connection.cursor(as_dict=True)
        cursor.execute("select GETDATE() as d;")
        return connection

    def import_schema(self, db_name: str = None, options: Options = None) -> Database:
        connection = self.connect()

        if db_name is None:
            db_name = self.database

        database = Database(naming.string_to_name(db_name))
        database.imported_db_type = "mssql"

        cursor = connection.cursor(as_dict=True)

        if options["exclude-udts"]:
            print("Skipping uddts...")
        else:
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
                udt = UDDT(name=QualifiedName.create(row["schema_name"], row["name"]),
                           required=row["is_nullable"] == 0,
                           native_type=row["base_type"])
                self.get_field_type_defaults(database, row["base_type"], udt, row["max_length"], row["precision"],
                                             row["scale"], None)
                database.uddts.append(udt)

        if options["exclude-tables"]:
            print("Skipping tables...")
        else:
            print("Processing tables...")
            cursor.execute(
                "select schema_name(tab.schema_id) as schema_name, tab.name as table_name, col.column_id as id, col.name, "
                "t.name as data_type, col.max_length, col.precision, col.is_nullable, "
                "COLUMNPROPERTY(object_id(schema_name(tab.schema_id)+'.'+tab.name), col.name, 'IsIdentity') as IS_IDENTITY, d.definition as default_value "
                "from sys.tables as tab "
                "inner join sys.columns as col on tab.object_id = col.object_id "
                "left join sys.types as t on col.user_type_id = t.user_type_id "
                "left join sys.default_constraints d on d.object_id = col.default_object_id "
                "order by tab.schema_id, tab.name, column_id;")
            table_name = "none"
            table = None
            for row in cursor.fetchall():
                new_table_name = f"{row["schema_name"]}.{row["table_name"]}"
                if table_name != new_table_name:
                    print(new_table_name)
                    table_name = new_table_name
                    table = Table(QualifiedName.create(row["schema_name"], row["table_name"]))
                    database.tables.append(table)

                field = Field(QualifiedName.create(row["schema_name"], row["name"]),
                              auto_increment=row["IS_IDENTITY"] == 1,
                              required=row["is_nullable"], native_type=row["data_type"])
                self.get_field_type_defaults(database, row["data_type"], field, row["max_length"], row["precision"],
                                             row["precision"], row["default_value"])

                table.fields.append(field)

        if options["exclude-views"]:
            print("Skipping views...")
        else:
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
                    view = View(QualifiedName.create(row["schema_name"], row["view_name"]))
                    view.definition = row["definition"]
                    database.views.append(view)

                if "." in row["name"]:
                    names = (str(row["name"])).split(".")
                    field = Field(
                        QualifiedName.create(names[0], names[1]),
                        required=row["is_nullable"], native_type=row["data_type"])
                else:
                    field = Field(
                        QualifiedName.create("", row["name"]),
                        required=row["is_nullable"],
                        native_type=row["data_type"])

                self.get_field_type_defaults(database, row["data_type"], field, row["max_length"], row["precision"],
                                             row["precision"], None)

                view.fields.append(field)

        if options["exclude-udts"]:
            print("Skipping udtts...")
        else:
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
                udtt = UDTT(QualifiedName.create(row["schema_name"], row["Type Name"]))
                database.udtts.append(udtt)

                field = Field(QualifiedName.create(row["schema_name"], row["Column"]),
                              required=row["Nullable"] == 0,
                              native_type=row["Data Type"])
                # is uddt?
                t = database.get_type(row["Data Type"])
                if t is not None:
                    field.generic_type = t.name.name.raw()
                else:
                    self.get_field_type_defaults(database, row["Data Type"], field, row["Length"], row["Precision"],
                                                 row["Scale"], None)

                udtt.fields.append(field)

        if options["exclude-functions"]:
            print("Skipping functions...")
        else:
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
                f = Function(QualifiedName.create(row["schema_name"], row["name"]), row["definition"],
                             FunctionType.from_str(row["type"]))
                database.functions.append(f)

        if options["exclude-storedprocedures"]:
            print("Skipping stored procedures...")
        else:
            print("Processing stored procedures...")
            cursor.execute(
                "select schema_name(schema_id) as schema_name, name, object_definition(object_id) as text from sys.procedures")

            for row in cursor.fetchall():
                print(f"{row["schema_name"]}.{row["name"]}")
                sp = StoredProcedure(QualifiedName.create(row["schema_name"], row["name"]), row["text"])
                database.stored_procedures.append(sp)

        if options["exclude-foreignkeys"]:
            print("Skipping foreign keys...")
        else:
            print("Processing foreign keys...")
            cursor.execute("SELECT  obj.name AS FK_NAME, "
                           "schema_name(tab1.schema_id) AS [schema_name], tab1.name AS [table], col1.name AS [column], "
                           "SCHEMA_NAME(tab2.schema_id) as ref_schema_name, tab2.name AS [referenced_table], "
                           "col2.name AS [referenced_column] "
                           "FROM sys.foreign_key_columns fkc "
                           "INNER JOIN sys.objects obj ON obj.object_id = fkc.constraint_object_id "
                           "INNER JOIN sys.tables tab1 ON tab1.object_id = fkc.parent_object_id "
                           "INNER JOIN sys.columns col1 ON col1.column_id = parent_column_id AND col1.object_id = tab1.object_id "
                           "INNER JOIN sys.tables tab2 ON tab2.object_id = fkc.referenced_object_id "
                           "INNER JOIN sys.columns col2 ON col2.column_id = referenced_column_id AND col2.object_id = tab2.object_id "
                           "order by obj.name")

            fk = None
            fk_name = ""
            new_fk_name = ""
            for row in cursor.fetchall():
                print(f"{row["schema_name"]}.{row["FK_NAME"]}")
                new_fk_name = f"{row["schema_name"]}.{row["FK_NAME"]}"
                if fk_name != new_fk_name:
                    fk = Key(QualifiedName.create(row["schema_name"],
                                                  row["FK_NAME"]), KeyType.ForeignKey)
                    fk.primary_table = QualifiedName.create(row["schema_name"],
                                                            row["table"])
                    fk.referenced_table = QualifiedName.create(row["ref_schema_name"],
                                                               row["referenced_table"])
                    table = database.get_table(QualifiedName.create(row["schema_name"],
                                                                    row["table"]))
                    table.foreign_keys.append(fk)

                    database.dependancies.append(Dependancy(fk.primary_table, fk.referenced_table))

                    fk_name = new_fk_name

                fk.primary_fields.append(row["column"])
                fk.fields.append(row["referenced_column"])

        if options["exclude-constraints"]:
            print("Skipping constraints...")
        else:
            print("Processing constraints...")
            cursor.execute(
                "select st.name as table_name, SCHEMA_NAME(st.schema_id) as schema_name,  chk.definition, "
                "chk.name as constraint_name, chk.type "
                "from sys.check_constraints chk "
                "inner join sys.columns col on chk.parent_object_id = col.object_id "
                "inner join sys.tables st on chk.parent_object_id = st.object_id "
                "order by st.name, col.column_id")

            for row in cursor.fetchall():
                print(f"{row["schema_name"]}.{row["constraint_name"]}")
                table = database.get_table(QualifiedName.create(row["schema_name"],
                                                                row["table_name"]))
                con = Constraint(QualifiedName.create(row["schema_name"],
                                                      row["constraint_name"]),
                                 QualifiedName.create(row["schema_name"],
                                                      row["table_name"]), row["definition"])
                table.constraints.append(con)

        if options["exclude-primarykeys"]:
            print("Skipping primary keys...")
        else:
            print("Processing primary keys...")
            cursor.execute(
                "SELECT ku.TABLE_SCHEMA, KU.table_name as TABLENAME ,column_name as PRIMARYKEYCOLUMN, tc.CONSTRAINT_NAME "
                "FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC "
                "INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU ON TC.CONSTRAINT_TYPE = 'PRIMARY KEY' "
                "AND TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME "
                "ORDER BY KU.TABLE_NAME ,KU.ORDINAL_POSITION")

            pk = None
            pk_name = ""
            new_pk_name = ""
            for row in cursor.fetchall():
                print(f"{row["TABLE_SCHEMA"]}.{row["TABLENAME"]}")
                new_pk_name = f"{row["TABLE_SCHEMA"]}.{row["TABLENAME"]}"
                if pk_name != new_pk_name:
                    pk = Key(QualifiedName.create(row["TABLE_SCHEMA"],
                                                  row["CONSTRAINT_NAME"]), KeyType.PrimaryKey)
                    pk.primary_table = QualifiedName.create(row["TABLE_SCHEMA"],
                                                            row["TABLENAME"])
                    table = database.get_table(QualifiedName.create(row["TABLE_SCHEMA"],
                                                                    row["TABLENAME"]))
                    table.pk = pk
                    pk_name = new_pk_name

                pk.fields.append(row["PRIMARYKEYCOLUMN"])

        if options["exclude-dependencies"]:
            print("Skipping dependencies...")
        else:
            print("Processing dependencies...")
            cursor.execute(
                "SELECT OBJECT_NAME(referencing_id) AS entity_name, SCHEMA_NAME(o.schema_id) as entity_schema, "
                "o.type as entity_type, referenced_entity_name, referenced_schema_name, ref.type as referenced_type "
                "FROM sys.sql_expression_dependencies AS sed "
                "INNER JOIN sys.objects AS o ON sed.referencing_id = o.object_id "
                "inner join sys.objects as ref on ref.object_id = referenced_id "
                "where ref.type in ('P', 'FN') "
                "and o.type in ('P', 'FN')")

            for row in cursor.fetchall():
                print(
                    f"{row["entity_schema"]}.{row["entity_name"]} => {row["referenced_schema_name"]}.{row["referenced_entity_name"]}")

                obj = database.get_object(QualifiedName.create(row["entity_schema"],
                                                               row["entity_name"]),
                                          self.get_object_type(row["entity_type"]))
                if obj is None:
                    raise DataException("Couldn't find object!")

                ref = database.get_object(
                    QualifiedName.create(row["referenced_schema_name"],
                                         row["referenced_entity_name"]),
                    self.get_object_type(row["referenced_type"]))
                if ref is None:
                    raise DataException("Couldn't find object!")
                dep = Dependancy(obj.name, ref.name, self.get_object_type(row["referenced_type"]))
                database.dependancies.append(dep)

            print("Processing udtt dependencies...")
            cursor.execute(
                "Select distinct SPECIFIC_SCHEMA, SPECIFIC_NAME, USER_DEFINED_TYPE_SCHEMA, USER_DEFINED_TYPE_NAME "
                "From Information_Schema.PARAMETERS "
                "Where USER_DEFINED_TYPE_NAME is not null "
                "order by SPECIFIC_SCHEMA, SPECIFIC_NAME")

            for row in cursor.fetchall():
                print(
                    f"{row["SPECIFIC_SCHEMA"]}.{row["SPECIFIC_NAME"]} => {row["USER_DEFINED_TYPE_SCHEMA"]}.{row["USER_DEFINED_TYPE_NAME"]}")
                obj = database.get_object(QualifiedName.create(row["SPECIFIC_SCHEMA"], row["SPECIFIC_NAME"]),
                                          "StoredProcedure")
                if obj is None:
                    raise DataException("Couldn't find object!")
                ref_type = "UDTT"
                ref = database.get_object(
                    QualifiedName.create(row["USER_DEFINED_TYPE_SCHEMA"], row["USER_DEFINED_TYPE_NAME"]),
                    self.get_object_type("UDTT"))
                if ref is None:
                    ref = database.get_object(
                        QualifiedName.create(row["USER_DEFINED_TYPE_SCHEMA"],
                                      row["USER_DEFINED_TYPE_NAME"]),
                        self.get_object_type("UDDT"))
                    ref_type = "UDDT"
                    if ref is None:
                        raise DataException("Couldn't find object!")

                dep = Dependancy(obj.name, ref.name, ref_type)
                database.dependancies.append(dep)

        connection.close()
        return database

    def calculate_sp_dependancies(self, database: Database) -> List[StoredProcedure]:
        dependancies = {d.obj: [] for d in database.dependancies}
        for item in database.dependancies:
            dependancies[item.obj].append(item.referenced_obj)

        graph = dict(zip(dependancies.keys(), map(set, dependancies.values())))
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
                f.write(self.generate_create_script(table, database.imported_db_type))
                f.flush()
            counter += 1

        print("Writing drop sp scripts....")
        local_path = os.path.join(path, "sp")
        create_dir(local_path, delete=True)

        # calculating dependencies
        stored_procs = self.calculate_sp_dependancies(database)
        reversed_sp = stored_procs[:]
        reversed_sp.reverse()

        with open(os.path.join(local_path, "drop_sp.sql"), "w", 1024, encoding="utf8") as f:
            for sp in reversed_sp:
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
            with open(os.path.join(local_path, udt.name.name.raw() + ".sql"), "w", 1024, encoding="utf8") as f:
                f.write(self.generate_create_uddt_script(udt, database.imported_db_type))
                f.flush()

        print("Writing UDTT scripts....")
        local_path = os.path.join(path, "udtt")
        create_dir(local_path)
        for udt in database.udtts:
            with open(os.path.join(local_path, udt.name.name.raw() + ".sql"), "w", 1024, encoding="utf8") as f:
                f.write(self.generate_create_udtt_script(udt, database.imported_db_type))
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
        elif name == "UDDT":
            return "UDDT"
        elif name == "U":
            return "Table"
        elif name == "V":
            return "View"
        else:
            raise DataException("Couldn't find type!")

    @staticmethod
    def get_field_size(field: Field | UDDT) -> str:
        if field.generic_type == "string":
            return f"({field.size})"
        elif field.generic_type == "decimal":
            return f"({field.size},{field.scale})"
        return ""

    @staticmethod
    def get_field_default(field: Field) -> str:
        if field.generic_type == "string" or field.generic_type == "datetime":
            return f"'{field.default}'"
        return field.default

    def generate_create_script(self, table: Table, original_db_type: str) -> str:
        sql: list[str] = []
        for field in table.fields:
            field_sql = (f"[{field.name}] {self.get_field_type(field, original_db_type)}"
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

    def generate_create_uddt_script(self, udt: UDDT, original_db_type: str) -> str:
        sql = f"CREATE TYPE {udt.name.schema}.{udt.name.name} FROM "
        f"[{udt.generic_type}] {self.get_field_type(udt, original_db_type)}"
        f"{self.get_field_size(udt)} {'NOT NULL' if udt.required else 'NULL'}"
        return sql

    def generate_create_udtt_script(self, udt: UDTT, original_db_type: str) -> str:
        sql = f"CREATE TYPE [{udt.name.schema}].[{udt.name.name}] AS TABLE ("

        field_sql = [f"[{field.name.name}] {self.get_field_type(field, original_db_type)} "
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
        elif value == "boolean" or value == "bool" or value == "bit":
            field.generic_type = "boolean"
            field.size = 1
        elif value == "decimal" or value == "money" or value == "numeric" or value == "smallmoney":
            field.generic_type = "decimal"
            field.size = precision
            field.scale = scale
        elif value == "string" or value == "varchar" or value == "nvarchar" or value == "char" or value == "nchar":
            field.generic_type = "string"
            field.size = size
        elif value == "varbinary" or value == "text" or value == "xml":
            field.generic_type = "binary"
            field.size = size
        elif value == "datetime" or value == "date" or value == "datetime2" or value == "smalldatetime" or value == "timestamp" or value == "time":
            field.generic_type = "datetime"
            field.size = 0
        elif value == "none" or value == "undefined":
            field.generic_type = "undefined"
            field.size = 0
        elif value == "uniqueidentifier":
            field.generic_type = "uniqueidentifier"
            field.size = 0
        elif value == "sysname":
            field.generic_type = "string"
            field.size = 128
        elif value == "hierarchyid":
            field.generic_type = "hierarchy"
            field.size = 1
        else:
            uddt = database.get_type(value)
            if uddt is None:
                raise DatatypeException("Unknown field type {}".format(value))
            else:
                field.generic_type = uddt.generic_type

        field.default = default

    def get_field_type(self, field: Field | UDDT, original_db_type: str) -> str:
        if original_db_type == "mssql" and field.native_type is not None:
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
        elif field.generic_type == "uniqueidentifier":
            return "UNIQUEIDENTIFIER"
        else:
            raise DatatypeException("Unknown field type ")

    def escape_field_list(self, values: List[str]) -> List[str]:
        return [f"`{value}`" for value in values]
