from enum import Enum, auto
from typing import List

from sb_serializer import Name


class DataException(Exception):
    pass


class DatatypeException(DataException):
    pass


class FieldType:
    pass


class FieldType(Enum):
    Undefined = 0
    Integer = 1
    String = 2
    Float = 3
    Decimal = 4
    Datetime = 5
    Boolean = 6
    Item = 7
    ListOfItem = 8

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
        elif value == "__item__":
            return FieldType.Item
        elif value == "[__item__]":
            return FieldType.ListOfItem
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
        elif self == FieldType.Item:
            return "__Item__"
        elif self == FieldType.ListOfItem:
            return "[__Item__]"
        elif self == FieldType.Undefined:
            return "None"
        else:
            raise DatatypeException("Unknown field type ")


class Field(object):
    name: Name
    type: FieldType
    size: int
    scale: int
    auto_increment: bool
    default: str
    required: bool

    def __init__(self, name: Name = None, field_type: FieldType = FieldType.Undefined, size: int = 0, scale: int = 0,
                 auto_increment: bool = False, default=None, required: bool = False):
        self.name = name
        self.type = field_type
        self.size = size
        self.scale = scale
        self.auto_increment = auto_increment
        self.default = default
        self.required = required

    def __str__(self):
        return f"{str(self.name)} {self.type} ({self.size},{self.scale}) {'AUTOINC ' if self.auto_increment else ''}" \
               f"{'DEFAULT ' + self.default + ' ' if self.default else ''}{'NOT NULL' if self.required else 'NULL'}"


class KeyType:
    pass


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
    name: Name
    fields: List[str]
    primary_table: str
    primary_fields: List[str]
    referenced_table: str
    key_type: KeyType

    def __init__(self, name: Name = None, key_type: KeyType = KeyType.Undefined, fields: list[str] = [],
                 primary_table: str = "", primary_fields: list[str] = []):
        self.name = name
        self.fields: List[str] = fields
        self.primary_table = primary_table
        self.primary_fields: List[str] = primary_fields
        self.key_type = key_type
        self.referenced_table = ""

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
    field: Field
    asc_or_desc: AscendingOrDescendingType

    def __init__(self, field: Field = None, asc_or_desc: AscendingOrDescendingType = AscendingOrDescendingType.Unknown):
        self.field = field
        self.asc_or_desc = asc_or_desc

    def __str__(self):
        return f"\"{self.field.name.raw()}\" "


class Table(object):
    name: Name
    schema: Name
    fields: list[Field]
    pk: Key
    keys: list[Key]
    foreign_keys: list[Key]

    def __init__(self, name: Name = None, schema: Name = None, fields: list[Field] = [], pk: Key = None, keys: list[Key] = [],
                 foreign_keys: list[Key] = []):
        self.name = name
        self.schema = schema
        self.fields: list[Field] = fields
        self.pk: Key | None = pk
        self.keys: list[Key] = keys
        self.foreign_keys: list[Key] = foreign_keys

    def find_field(self, name: str) -> Field:
        found_fields = [f for f in self.fields if f.name.raw().lower() == name.lower()]
        if len(found_fields) > 0:
            return found_fields[0]
        else:
            raise DataException("Could not find field")

    def __str__(self):
        return str(self.name)


class View(Table):
    order_by: list[OrderByField]


class StoredProcedure:
    text: str


class UDT:
    name: str
    type: FieldType
    size: int
    scale: int
    required: bool

    def __init__(self, name: str = None, type: FieldType = FieldType.Undefined, size: int = 0, scale: int = 0, required: bool = False):
        self.name = name
        self.type = type
        self.size = size
        self.scale = scale
        self.required = required


class Database(object):
    name: Name
    tables: List[Table]
    stored_procedures: List[StoredProcedure]
    udts: List[UDT]

    def __init__(self, name: Name = None):
        self.name = name
        self.tables: List[Table] = []
        self.stored_procedures: List[StoredProcedure] = []
        self.udts: List[UDT] = []

    def get_table(self, name: str):
        result = [table for table in self.tables if table.name.raw() == name]
        if len(result) > 0:
            return result[0]
        return None

    def get_stored_procedure(self, name: str):
        result = [sp for sp in self.stored_procedures if sp.name == name]
        if len(result) > 0:
            return result[0]
        return None
