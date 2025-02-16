from enum import Enum, auto, Flag
from typing import List

from sb_serializer import Name, Naming


class DataException(Exception):
    pass


class DatatypeException(DataException):
    pass


class QualifiedName:
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


class FieldType:
    pass


class FieldType(Flag):
    Undefined = auto()
    Integer = auto()
    String = auto()
    Float = auto()
    Decimal = auto()
    Datetime = auto()
    Boolean = auto()
    UniqueIdentifier = auto()
    Binary = auto()
    Hierarchy = auto()

    @classmethod
    def get_fieldtype(cls, value: str) -> FieldType:
        value = value.lower()
        if value == "integer" or value == "int" or value == "bigint" or value == "tinyint":
            return FieldType.Integer
        elif value == "string" or value == "varchar" or value == "char" or value == "text":
            return FieldType.String
        elif value == "float" or value == "real":
            return FieldType.Float
        elif value == "datetime" or value == "date":
            return FieldType.Datetime
        elif value == "boolean" or value == "bool":
            return FieldType.Boolean
        elif value == "decimal" or value == "money":
            return FieldType.Decimal
        elif value == "binary":
            return FieldType.Binary
        elif value == "uniqueidentifier":
            return FieldType.UniqueIdentifier
        elif value == "none":
            return FieldType.Undefined
        else:
            raise DatatypeException(f"Unknown field type {value}")

    def __str__(self):
        if self == FieldType.Integer:
            return "Integer"
        elif self == FieldType.String:
            return "String"
        elif self == FieldType.Float:
            return "Float"
        elif self == FieldType.Datetime:
            return "Datetime"
        elif self == FieldType.Boolean:
            return "Boolean"
        elif self == FieldType.Decimal:
            return "Decimal"
        elif self == FieldType.Binary:
            return "Binary"
        elif self == FieldType.UniqueIdentifier:
            return "UniqueIdentifier"
        elif self == FieldType.Undefined:
            return "None"
        else:
            raise DatatypeException("Unknown field type ")


class SchemaObject:
    name: QualifiedName

    def __init__(self, name: QualifiedName = None):
        self.name = name

    def __str__(self):
        return str(self.name)


class UDDT(SchemaObject):
    name: QualifiedName
    generic_type: FieldType
    size: int
    scale: int
    required: bool
    native_type: str

    def __init__(self, name: QualifiedName = None, generic_type: FieldType = FieldType.Undefined, size: int = 0,
                 scale: int = 0, required: bool = False, native_type: str = None):
        super().__init__(name)
        self.generic_type = generic_type
        self.size = size
        self.scale = scale
        self.required = required
        self.native_type = native_type

    def __str__(self):
        return str(self.name)


class Field(SchemaObject):
    name: Name
    generic_type: FieldType
    size: int
    scale: int
    auto_increment: bool
    default: str
    required: bool
    native_type: str

    def __init__(self, name: Name = None, generic_type: FieldType  = FieldType.Undefined, size: int = 0,
                 scale: int = 0, auto_increment: bool = False, default=None, required: bool = False,
                 native_type: str = None):
        self.name = name
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


class Key(object):
    name: QualifiedName
    fields: List[str]
    primary_table: QualifiedName
    primary_fields: List[str]
    referenced_table: QualifiedName
    key_type: KeyType

    def __init__(self, name: QualifiedName = None, key_type: KeyType = KeyType.Undefined, fields: list[str] = [],
                 primary_table: QualifiedName = None, primary_fields: list[str] = []):
        self.name = name
        self.fields: List[str] = fields
        self.primary_table = primary_table
        self.primary_fields: List[str] = primary_fields
        self.key_type = key_type
        self.referenced_table = None

    def __str__(self):
        return f"{self.name} {self.key_type} {','.join(self.fields)}{self.primary_table}" \
               f"{'' if len(self.primary_fields) == 0 else ','.join(self.primary_fields)}"


class AscendingOrDescendingType:
    pass


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


class Constraint:
    name: QualifiedName
    table_name: QualifiedName
    definition: str

    def __init__(self, name: QualifiedName = None, table_name: QualifiedName = None, definition: str = ""):
        self.name = name
        self.table_name = table_name
        self.definition = definition

    def __str__(self):
        return str(self.name)


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


class View(Table):
    definition: str


class StoredProcedure(SchemaObject):
    text: str

    def __init__(self, name: QualifiedName = None, text: str = None):
        super().__init__(name)
        self.text = text


class UDTT(SchemaObject):
    name: QualifiedName
    fields: list[Field]

    def __init__(self, name: QualifiedName = None, fields: list[Field] = []):
        super().__init__(name)
        self.fields: list[Field] = fields

    def find_field(self, name: str) -> Field:
        found_fields = [f for f in self.fields if f.name.lower() == name.lower()]
        if len(found_fields) > 0:
            return found_fields[0]
        else:
            raise DataException("Could not find field")


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


class Database(object):
    name: Name
    tables: List[Table]
    stored_procedures: List[StoredProcedure]
    functions: List[Function]
    uddts: List[UDDT]
    udtts: List[UDTT]
    dependencies: List[Dependancy]

    def __init__(self, name: Name = None):
        self.name = name
        self.tables: List[Table] = []
        self.stored_procedures: List[StoredProcedure] = []
        self.functions: List[Function] = []
        self.uddts: List[UDDT] = []
        self.udtts: List[UDTT] = []
        self.dependencies: List[Dependancy] = []

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

    def get_type(self, name: str) -> UDDT | None:
        result = [t for t in self.uddts if t.name.name.lower() == name.lower()]
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

        self.dependencies = self.dependencies[:count]
