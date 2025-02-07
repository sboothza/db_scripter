import re
import sqlite3
from typing import Union, List

from database_objects import Database, Table, KeyType, FieldType, Key, Field, DatatypeException, DataException
from adaptor import Adaptor
from common import get_fullname, get_filename, clean_string, find_in_list


class SqliteAdaptor(Adaptor):
    """ Connection string is sqlite://filename or sqlite://memory """

    def __init__(self, connection, naming):
        super().__init__(connection, naming)
        connection_string = self.connection.replace("sqlite://", "")
        if connection_string == "memory":
            self.connection = "file::memory:?cache=shared"
        else:
            self.connection = get_fullname(connection_string)

    def import_schema(self, db_name: str) -> Database:
        connection = sqlite3.connect(self.connection)
        if db_name is None:
            db_name = get_filename(self.connection)

        database = Database(self.naming.string_to_name(db_name))

        cursor = connection.execute("SELECT sql FROM sqlite_master WHERE type='table'", [])
        for row in cursor.fetchall():
            create_script = row[0]
            table = self.parse_create_script(create_script)
            if table is not None:
                database.tables.append(table)

        cursor.close()

        return database

    def escape_field_list(self, values: List[str]) -> List[str]:
        return ["\"" + value + "\"" for value in values]

    def generate_create_script(self, table: Table) -> str:
        sql: list[str] = []
        for field in table.fields:
            sql.append(f"\"{field.name.raw()}\" {self.get_field_type(field.generic_type)} ")
        if table.pk:
            sql.append(f"PRIMARY KEY ({','.join(self.escape_field_list(table.pk.fields))})")

        for fk in [key for key in table.keys if key.key_type == KeyType.ForeignKey]:
            sql.append(f"FOREIGN KEY ({','.join(self.escape_field_list(fk.fields))}) REFERENCES "
                       f"\"{fk.primary_table}\"({','.join(self.escape_field_list(fk.primary_fields))})")

        joiner = ',\n\t'
        result = f"create table \"{table.name.raw()}\" (\n\t{joiner.join(sql)}\n);\n"

        for key in table.keys:
            if key.key_type == KeyType.Unique:
                result += f" CREATE UNIQUE INDEX \"{key.name.raw()}\" ON \"{table.name.raw()}\" " \
                          f"({','.join(self.escape_field_list(key.fields))});\n"

            elif key.key_type == KeyType.Index:
                result += f" CREATE INDEX \"{key.name.raw()}\" ON \"{table.name.raw()}\" " \
                          f"({','.join(self.escape_field_list(key.fields))});\n"

        return result

    def generate_table_exists_script(self, table: Table, db_name: str) -> str:
        return f"SELECT name FROM sqlite_schema WHERE type='table' and name = '{table.name.raw()}'"

    def generate_drop_script(self, table: Table) -> str:
        return f"drop table {table.name.raw()};"

    def generate_count_script(self, table: Table) -> str:
        return f"select count(*) from {table.name.raw()}"

    def generate_insert_script(self, table: Table) -> str:
        fields = [f.name.raw() for f in table.fields if not f.auto_increment]
        params = ", ".join([f"{f}" for f in fields])
        values = ", ".join([":" + f for f in fields])
        result = f"insert into {table.name.raw()} ({params}) values ({values});"
        return result

    def generate_update_script(self, table: Table) -> str:
        fields = [f.name.raw() for f in table.fields if not f.auto_increment and f.name not in table.pk.fields]
        update_list = [f"{f} = :{f}" for f in fields]
        update = ", ".join(update_list)
        key_list = [f"{f} = :{f}" for f in table.pk.fields]
        key = " and ".join(key_list)
        result = f"update {table.name.raw()} set {update} where {key};"
        return result

    def generate_delete_script(self, table: Table) -> str:
        key_list = [f"{f} = :{f}" for f in table.pk.fields]
        key = " and ".join(key_list)
        result = f"delete from {table.name.raw()} where {key};"
        return result

    def generate_fetch_by_id_script(self, table: Table) -> str:
        field_list = [f.name.raw() for f in table.fields]
        fields = ", ".join([f"{f}" for f in field_list])
        key_list = [f"{f} = :{f}" for f in table.pk.fields]
        key = " and ".join(key_list)
        result = f"select {fields} from {table.name.raw()} where {key};"
        return result

    def generate_item_exists_script(self, table: Table) -> str:
        key_list = [f"{f} = :{f}" for f in table.pk.fields]
        key = " and ".join(key_list)
        result = f"select count(*) from {table.name.raw()} where {key};"
        return result

    def get_field_type(self, field_type: FieldType) -> str:
        if field_type == FieldType.Integer:
            return "INTEGER"
        elif field_type == FieldType.String:
            return "TEXT"
        elif field_type == FieldType.Float or field_type == FieldType.Decimal:
            return "REAL"
        elif field_type == FieldType.Datetime:
            return "REAL"
        elif field_type == FieldType.Boolean:
            return "INTEGER"
        else:
            raise DatatypeException("Unknown field type ")

    def must_remap_field(self, field_type: FieldType) -> tuple[bool, FieldType]:
        if field_type == FieldType.Integer:
            return False, FieldType.Integer
        elif field_type == FieldType.String:
            return False, FieldType.String
        elif field_type == FieldType.Float or field_type == FieldType.Decimal:
            return False, FieldType.Float
        elif field_type == FieldType.Datetime:
            return True, FieldType.Float
        elif field_type == FieldType.Boolean:
            return True, FieldType.Integer
        else:
            raise DatatypeException("Unknown field type ")

    def parse_create_script(self, create_script: str) -> Union[Table, None]:
        table: Table
        # create_script = create_script.replace(",")
        lines = create_script.split("\n")
        ux_count = 1
        ix_count = 1
        table_name = ""

        for line in lines:
            line = clean_string(line)
            if "CREATE TABLE" in line:
                match = re.search(r"CREATE TABLE (\w+)\s*\(", line)
                if match:
                    table_name = match.group(1)
                    if table_name == "sqlite_sequence":
                        return None

                    table = Table(self.naming.string_to_name(table_name))
                else:
                    raise DataException("create table issue")

            elif line.strip() == ")":
                pass
            elif line.startswith("PRIMARY KEY"):
                match = re.search(r"PRIMARY KEY \((.*)\),?", line)
                if match:
                    fields = match.group(1).split(",")
                    pk = Key(self.naming.string_to_name(f"pk_{table_name}"), key_type=KeyType.PrimaryKey)
                    for fieldname in fields:
                        pk_field = table.find_field(fieldname)
                        if pk_field is None:
                            raise Exception(f"Couldn't find field {fieldname}")
                        pk.fields.append(fieldname)
                    table.pk = pk
                else:
                    raise DataException("pk issue")
            elif line.startswith("UNIQUE"):
                match = re.search(r"UNIQUE \((.*)\),?", line)
                if match:
                    fields = match.group(1).split(",")
                    ux = Key(self.naming.string_to_name(f"ux_{table_name}_{ux_count}"), key_type=KeyType.Unique)
                    ux_count = ux_count + 1
                    for fieldname in fields:
                        ux_field = table.find_field(fieldname)
                        if ux_field is None:
                            raise Exception(f"Couldn't find field {fieldname}")
                        ux.fields.append(fieldname)
                    table.keys.append(ux)
                else:
                    raise DataException("ux issue")
            elif line.startswith("CONSTRAINT"):
                match = re.search(r"CONSTRAINT (\w+) (PRIMARY KEY|FOREIGN KEY)(.*)", line)
                if match:
                    name = self.naming.string_to_name(match.group(1))
                    type = match.group(2)
                    remainder = match.group(3)
                    if type == "PRIMARY KEY":
                        remainder = remainder.replace("(", "").replace("),", "").replace(")", "").strip()
                        fields = remainder.split(",")
                        pk = Key(name, key_type=KeyType.PrimaryKey)
                        for fieldname in fields:
                            pk_field = table.find_field(fieldname)
                            if pk_field is None:
                                raise Exception(f"Couldn't find field {fieldname}")
                            pk.fields.append(fieldname)
                        table.pk = pk
                    elif type == "FOREIGN KEY":
                        refmatch = re.search((r"\(([^\)]+)\) REFERENCES (\w+)\W*\(([^\)]+)\)"), remainder)
                        if refmatch:
                            local_fields = refmatch.group(1).split(",")
                            remote_table = refmatch.group(2)
                            remote_fields = refmatch.group(3).split(",")
                            ref = Key(name, KeyType.ForeignKey)
                            ref.primary_table = remote_table
                            ref.referenced_table = table.name.raw()
                            ref.primary_fields = remote_fields
                            ref.fields = local_fields
                            table.keys.append(ref)
                    else:
                        raise DataException("constraint issue")
                else:
                    raise DataException("constraint issue")
            else:
                line = clean_string(line).lower()
                words = line.split()
                name = words[0]
                type = words[1]
                required = False
                auto_increment = False
                default_value = None

                if "not null" in line:
                    required = True
                    line = line.replace("not null", "")
                elif "null" in line:
                    required = False
                    line = line.replace("null", "")

                if "primary key" in line:
                    required = True
                    pk = Key(self.naming.string_to_name(f"pk_{table_name}"), key_type=KeyType.PrimaryKey)
                    pk.fields.append(name)
                    table.pk = pk
                    line = line.replace("primary key", "")

                if "unique" in line:
                    ux = Key(self.naming.string_to_name(f"ux_{table_name}_{ux_count}"), key_type=KeyType.Unique)
                    ux_count = ux_count + 1
                    ux.fields.append(name)
                    table.keys.append(ux)
                    line = line.replace("unique", "")

                if "autoincrement" in line:
                    auto_increment = True
                    line = line.replace("autoincrement", "")

                if "default" in line:
                    words = line.split()
                    index = find_in_list("default", words)
                    default_value = words[index + 1]

                field = Field(self.naming.string_to_name(name), FieldType.get_fieldtype(type), required=required,
                              auto_increment=auto_increment, default=default_value)
                table.fields.append(field)

        if table.pk is not None and len(table.pk.fields) == 1:
            pk_field = table.find_field(table.pk.fields[0])
            if pk_field.generic_type == FieldType.Integer:
                pk_field.auto_increment = True

        return table

    def replace_parameters(self, query: str) -> str:
        return re.sub(r"::(\w+)::", r":\1", query)

    def build_selection_list(self, fields: list) -> str:
        field_list = []
        for field in fields:
            if field.table_alias != "":
                field_str = f"{field.table_alias}.\"{field.name}\""
            else:
                field_str = f"\"{field.name}\""
            field_list.append(field_str)
        return str.join(", ", field_list)
