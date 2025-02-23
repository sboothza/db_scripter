from enum import Enum, auto
from typing import List

from sb_serializer import Name


class DataException(Exception):
    ...


class DatatypeException(DataException):
    ...


class QualifiedName:
    """
    QualifiedName - schema + name
    """
    schema: Name
    name: Name

    def __init__(self, schema: Name = None, name: Name = None):
        self.schema = schema
        self.name = name

    def __str__(self):
        return f"{self.schema.pascal()}.{self.name.pascal()}"

    def __gt__(self, other):
        return str(self) > str(other)

    def __lt__(self, other):
        return str(self) < str(other)

    def __eq__(self, other):
        return str(self) == str(other)

    def __hash__(self):
        return hash(str(self))


class OperationType(Enum):
    Create = 1
    Drop = 2
    Modify = 3
    Retain = 4


class SchemaObject:
    """
    Base class for schema entities - tables, views, sp, everything
    Stores a name (schema + name)
    """
    name: QualifiedName
    operation: OperationType

    def __init__(self, name: QualifiedName = None, operation: OperationType = OperationType.Retain):
        self.name = name
        self.operation = operation

    def __str__(self):
        return str(self.name)

    def __eq__(self, other):
        return self.name == other.name

    def __hash__(self):
        return hash(self.name)

    def finalise(self):
        """
        sort lists, do cleanups
        :return:
        """
        ...


# class FieldType(SchemaObject):
#     def __init__(self, name: QualifiedName = None):
#         super().__init__(name)


class UDDT(SchemaObject):
    """
    User defined data type
    """
    generic_type: str
    size: int
    scale: int
    required: bool
    native_type: QualifiedName

    def __init__(self, name: QualifiedName = None, generic_type: str = None, size: int = 0,
                 scale: int = 0, required: bool = False, native_type: QualifiedName = None):
        super().__init__(name)
        self.generic_type = generic_type
        self.size = size
        self.scale = scale
        self.required = required
        self.native_type = native_type

    def __str__(self):
        return str(self.name)

    def __eq__(self, other):
        return (self.name == other.name and self.generic_type == other.generic_type and self.size == other.size
                and self.scale == other.scale and self.required == other.required
                and self.native_type == other.native_type)

    def __hash__(self):
        return hash((self.name, self.generic_type, self.size, self.scale, self.required, self.native_type))


class Field(SchemaObject):
    """
    Field or column of a table or view
    """

    generic_type: str
    """
        generic_type - db agnostic field type
    """

    native_type: QualifiedName
    """
        native_type - native db type as imported
    """
    size: int
    scale: int
    auto_increment: bool
    default: str
    required: bool

    def __init__(self, name: QualifiedName = None, generic_type: str = None,
                 size: int = 0,
                 scale: int = 0, auto_increment: bool = False, default=None, required: bool = False,
                 native_type: QualifiedName = None):
        super().__init__(name)
        self.generic_type = generic_type
        self.size = size
        self.scale = scale
        self.auto_increment = auto_increment
        self.default = default
        self.required = required
        self.native_type = native_type

    def __str__(self):
        return f"{str(self.name)} {self.generic_type} ({self.size},{self.scale}) {'AUTOINC ' if self.auto_increment else ''}" \
               f"{'DEFAULT ' + self.default + ' ' if self.default else ''}{'NOT NULL' if self.required else 'NULL'}"

    def __eq__(self, other):
        return (self.name == other.name and self.generic_type == other.generic_type and self.size == other.size
                and self.scale == other.scale and self.auto_increment == other.auto_increment
                and self.default == other.default and self.required == other.required
                and self.native_type == other.native_type)

    def __hash__(self):
        return hash((self.name, self.generic_type, self.size, self.scale, self.auto_increment, self.default,
                     self.required, self.native_type))


class KeyType:
    ...


class KeyType(Enum):
    Undefined = 0
    PrimaryKey = 1
    Index = 2
    Unique = 3
    ForeignKey = 4
    Lookup = 5

    @classmethod
    def get_keytype(cls, value: str) -> KeyType:
        value = value.lower()
        if value == "undefined":
            return KeyType.Undefined
        elif value == "primarykey" or value == "primary key":
            return KeyType.PrimaryKey
        elif value == "index":
            return KeyType.Index
        elif value == "unique":
            return KeyType.Unique
        elif value == "foreignkey" or value == "foreign key":
            return KeyType.ForeignKey
        elif value == "lookup":
            return KeyType.Lookup
        else:
            raise DatatypeException(f"Unknown key type {value}")

    def __str__(self):
        if self == KeyType.Undefined:
            return "Undefined"
        elif self == KeyType.PrimaryKey:
            return "PrimaryKey"
        elif self == KeyType.Index:
            return "Index"
        elif self == KeyType.Unique:
            return "Unique"
        elif self == KeyType.ForeignKey:
            return "ForeignKey"
        elif self == KeyType.Lookup:
            return "Lookup"
        else:
            raise DatatypeException("Unknown key type ")


class Key(SchemaObject):
    fields: List[str]
    primary_table: QualifiedName
    primary_fields: List[str]
    referenced_table: QualifiedName
    key_type: KeyType

    def __init__(self, name: QualifiedName = None, key_type: KeyType = KeyType.Undefined):
        super().__init__(name)
        self.fields: List[str] = []
        self.primary_table = None
        self.primary_fields: List[str] = []
        self.key_type = key_type
        self.referenced_table = None

    def __str__(self):
        return f"{self.name} {self.key_type} {','.join(self.fields)}{self.primary_table}" \
               f"{'' if len(self.primary_fields) == 0 else ','.join(self.primary_fields)}"

    def __eq__(self, other):
        if other is not Key:
            return False

        return (self.name == other.name and self.fields == other.fields and self.primary_table == other.primary_table
                and self.primary_fields == other.primary_fields and self.referenced_table == other.referenced_table
                and self.key_type == other.key_type)

    def __hash__(self):
        return hash(
            (self.name, self.fields, self.primary_table, self.primary_fields, self.referenced_table, self.key_type))

    def finalise(self):
        self.fields.sort()


class AscendingOrDescendingType:
    ...


class AscendingOrDescendingType(Enum):
    Unknown = auto()
    Ascending = auto()
    Descending = auto()

    @staticmethod
    def to_string(asc_desc: AscendingOrDescendingType) -> str:
        if asc_desc == AscendingOrDescendingType.Ascending:
            return "ASC"
        elif asc_desc == AscendingOrDescendingType.Descending:
            return "DESC"
        else:
            raise DataException("Unknown order type")


class OrderByField(object):
    field: str
    asc_or_desc: AscendingOrDescendingType

    def __init__(self, field: str = None, asc_or_desc: AscendingOrDescendingType = AscendingOrDescendingType.Unknown):
        self.field = field
        self.asc_or_desc = asc_or_desc

    def __str__(self):
        return f"\"{self.field}\" "


class Dependancy:
    obj: QualifiedName
    referenced_obj: QualifiedName
    obj_type: str

    def __init__(self, obj: QualifiedName = None, referenced_obj: QualifiedName = None, obj_type: str = None):
        self.obj = obj
        self.referenced_obj = referenced_obj
        self.obj_type = obj_type

    def __str__(self):
        return str(self.obj)

    def __eq__(self, other):
        return (self.name == other.name and self.obj == other.obj and self.referenced_obj == other.referenced_obj
                and self.obj_type == other.obj_type)

    def __hash__(self):
        return hash((self.name, self.obj, self.referenced_obj, self.obj_type))


class Constraint(SchemaObject):
    table_name: QualifiedName
    definition: str

    def __init__(self, name: QualifiedName = None, table_name: QualifiedName = None, definition: str = ""):
        super().__init__(name)
        self.table_name = table_name
        self.definition = definition

    def __str__(self):
        return str(self.name)

    def __eq__(self, other):
        return self.name == other.name and self.table_name == other.table_name and self.definition == other.definition

    def __hash__(self):
        return hash((self.name, self.table_name, self.definition))


class Table(SchemaObject):
    fields: list[Field]
    pk: Key
    keys: list[Key]
    foreign_keys: list[Key]
    constraints: list[Constraint]

    def __init__(self, name: QualifiedName = None):
        super().__init__(name)
        self.fields: list[Field] = []
        self.pk: Key | None = None
        self.keys: list[Key] = []
        self.foreign_keys: list[Key] = []
        self.constraints: list[Constraint] = []

    def find_field(self, name: str) -> Field:
        found_fields = [f for f in self.fields if f.name == name.lower()]
        if len(found_fields) > 0:
            return found_fields[0]
        else:
            raise DataException("Could not find field")

    def __eq__(self, other):
        if other is not Table:
            return False

        return (self.name == other.name and self.fields == other.fields and self.pk == other.pk
                and self.keys == other.keys and self.foreign_keys == other.foreign_keys
                and self.constraints == other.constraints)

    def __hash__(self):
        return hash((self.name, self.fields, self.pk, self.keys, self.foreign_keys, self.constraints))

    def finalise(self):
        self.fields.sort()
        self.keys.sort()
        self.foreign_keys.sort()
        self.constraints.sort()


class View(Table):
    definition: str

    def __eq__(self, other):
        return self.name == other.name and self.definition == other.definition

    def __hash__(self):
        return hash((self.name, self.definition))


class StoredProcedure(SchemaObject):
    text: str

    def __init__(self, name: QualifiedName = None, text: str = None):
        super().__init__(name)
        self.text = text

    def __eq__(self, other):
        return self.name == other.name and self.text == other.text

    def __hash__(self):
        return hash((self.name, self.text))


class UDTT(SchemaObject):
    """
    User defined table type
    """

    fields: list[Field]

    def __init__(self, name: QualifiedName = None, fields: list[Field] = None):
        super().__init__(name)
        self.fields: list[Field] = (fields if fields is not None else [])

    def find_field(self, name: str) -> Field:
        found_fields = [f for f in self.fields if str(f.name).lower() == name.lower()]
        if len(found_fields) > 0:
            return found_fields[0]
        else:
            raise DataException("Could not find field")

    def __eq__(self, other):
        return self.name == other.name and self.fields == other.fields

    def __hash__(self):
        return hash((self.name, self.fields))

    def finalise(self):
        self.fields.sort()


class FunctionType:
    ...


class FunctionType(Enum):
    ScalarFunction = 1
    TableFunction = 2

    @staticmethod
    def from_str(name: str) -> FunctionType:
        if name == "function":
            return FunctionType.ScalarFunction
        elif name == "table function":
            return FunctionType.TableFunction
        else:
            raise DataException("Couldn't find type!")


class Function(SchemaObject):
    text: str
    type: FunctionType

    def __init__(self, name: QualifiedName = None, text: str = None,
                 type: FunctionType = FunctionType.ScalarFunction):
        super().__init__(name)
        self.text = text
        self.type = type

    def __eq__(self, other):
        return self.name == other.name and self.text == other.text and self.type == other.type

    def __hash__(self):
        return hash((self.name, self.text, self.type))


class Database:
    ...


class Database(object):
    name: Name
    tables: List[Table]
    views: List[View]
    stored_procedures: List[StoredProcedure]
    functions: List[Function]
    uddts: List[UDDT]
    udtts: List[UDTT]
    dependancies: List[Dependancy]
    imported_db_type: str

    def __init__(self, name: Name = None):
        self.name = name
        self.tables: List[Table] = []
        self.views: List[View] = []
        self.stored_procedures: List[StoredProcedure] = []
        self.functions: List[Function] = []
        self.uddts: List[UDDT] = []
        self.udtts: List[UDTT] = []
        self.dependancies: List[Dependancy] = []
        self.imported_db_type = ""

    def get_unknown_object(self, name: QualifiedName) -> SchemaObject:
        obj = self.get_table(name)
        if obj is None:
            obj = self.get_stored_procedure(name)

        if obj is None:
            obj = self.get_function(name)

        if obj is None:
            obj = self.get_type(name)

        if obj is None:
            obj = self.get_table_type(name)

        return obj

    def get_object(self, name: QualifiedName, type: str) -> SchemaObject | None:
        if type == "Table" or type == "View":
            return self.get_table(name)
        elif type == "StoredProcedure":
            return self.get_stored_procedure(name)
        elif type == "UDDT":
            return self.get_type(name.name.raw())
        elif type == "UDTT":
            return self.get_table_type(name)
        elif type == "Function":
            return self.get_function(name)
        else:
            raise DataException("Couldn't find type!")

    def get_table(self, name: QualifiedName) -> Table | None:
        result = [table for table in self.tables if
                  table.name.name.lower() == name.name.lower() and table.name.schema.lower() == name.schema.lower()]
        if len(result) > 0:
            return result[0]
        return None

    def get_stored_procedure(self, name: QualifiedName) -> StoredProcedure | None:
        result = [sp for sp in self.stored_procedures if
                  sp.name.name.lower() == name.name.lower() and sp.name.schema.lower() == name.schema.lower()]
        if len(result) > 0:
            return result[0]
        return None

    def get_function(self, name: QualifiedName) -> Function | None:
        result = [f for f in self.functions if f.name.name.lower() == name.name.lower()]
        if len(result) > 0:
            return result[0]
        return None

    def get_type(self, name: QualifiedName) -> UDDT | None:
        result = [t for t in self.uddts if t.name.name.lower() == name.name.lower()]
        if len(result) > 0:
            return result[0]
        return None

    def get_table_type(self, name: QualifiedName) -> UDTT | None:
        result = [t for t in self.udtts if t.name.name.lower() == name.name.lower()]
        if len(result) > 0:
            return result[0]
        return None

    def trim_db(self, count: int):
        self.tables = self.tables[:count]
        self.stored_procedures = self.stored_procedures[:count]
        self.udtts = self.udtts[:count]
        self.dependencies = [d for d in self.dependencies if
                             self.get_table(d.obj) is None or self.get_table(d.referenced_obj) is None]

        self.dependancies = self.dependancies[:count]

    def get_diff(self, target_database: Database):
        diff_db: Database = Database(target_database.name)

        # process: find new entities and create, existing entities not in new, drop, existing in both but different, modify
        diff_db.tables = self.diff_entity(self.tables, target_database.tables)
        diff_db.views = self.diff_entity(self.views, target_database.views)
        diff_db.stored_procedures = self.diff_entity(self.stored_procedures, target_database.stored_procedures)
        diff_db.functions = self.diff_entity(self.functions, target_database.functions)
        diff_db.udtts = self.diff_entity(self.udtts, target_database.udtts)
        diff_db.uddts = self.diff_entity(self.uddts, target_database.uddts)
        diff_db.dependancies = self.dependancies.extend(target_database.dependancies)
        diff_db.finalise()

    def clean_dependancies(self):
        new_dependencies: list[Dependancy] = []
        for dependency in self.dependancies:
            obj = self.get_unknown_object(dependency.obj)
            if obj is not None:
                obj = self.get_unknown_object(dependency.referenced_obj)
                if obj is not None:
                    new_dependencies.append(dependency)

        self.dependancies = new_dependencies

    def diff_entity(self, current: list[SchemaObject], new: list[SchemaObject]) -> list[SchemaObject]:
        # process: find new entities and create, existing entities not in new, drop, existing in both but different, modify

        diff_objs: list[SchemaObject] = []

        objects = [obj for obj in new if obj not in current]
        for obj in objects:
            obj.operation = OperationType.Create
        diff_objs.extend(objects)

        objects = [obj for obj in current if obj not in new]
        for obj in objects:
            obj.operation = OperationType.Drop
        diff_objs.extend(objects)

        # check for changes
        for obj in current:
            new_obj = self.find_entity(new, obj.name)
            if obj is not None and obj != new_obj:
                new_obj.operation = OperationType.Modify
                diff_objs.append(new_obj)

        return diff_objs

    def find_entity(self, objs: list[SchemaObject], name: QualifiedName) -> SchemaObject:
        result = [obj for obj in objs if obj.name == name]
        if len(result) > 0:
            return result[0]
        return None

    def finalise(self):
        self.tables.sort(key=lambda x: str(x.name).lower())
        self.views.sort(key=lambda x: str(x.name).lower())
        self.functions.sort(key=lambda x: str(x.name).lower())
        self.stored_procedures.sort(key=lambda x: str(x.name).lower())
        self.udtts.sort(key=lambda x: str(x.name).lower())
        self.dependancies.sort(key=lambda x: str(x.obj.name).lower())
        self.uddts.sort(key=lambda x: str(x.name).lower())
        self.clean_dependancies()


class Term(object):
    ...


class Expression(object):
    ...


class SelectStatement(object):
    fields: list[Field]
    tables: list[Table]
    where_clause: Expression
