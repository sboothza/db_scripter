import os.path
import shutil
from pathlib import Path

from sb_serializer import Naming, HardSerializer

from src.db_scripter.config import DICTIONARY_FILENAME, BIG_DICTIONARY_FILENAME
from src.db_scripter.database_objects import SchemaObject, OperationType

naming = Naming(DICTIONARY_FILENAME, BIG_DICTIONARY_FILENAME)
serializer = HardSerializer(naming=naming)


def create_dir(path: str, delete: bool = False):
    if not os.path.exists(path) or not delete:
        os.makedirs(path, exist_ok=True)
    else:
        shutil.rmtree(path)
        os.mkdir(path)


def is_str_char(c: str) -> bool:
    return c.isidentifier() or c == "[" or c == "]" or c == "." or c == "'"


def get_fullname(path: str) -> str:
    p = Path(path)
    p.resolve()
    return str(p.expanduser())


def get_filename(path: str) -> str:
    p = Path(path)
    p.resolve()
    return str(p.expanduser()).replace(str(p.parent), "").replace("/", "")


def clean_string(dirty: str) -> str:
    while "\n" in dirty:
        dirty = dirty.replace("\n", " ")

    while "\r" in dirty:
        dirty = dirty.replace("\r", " ")

    while "\t" in dirty:
        dirty = dirty.replace("\t", " ")

    while "  " in dirty:
        dirty = dirty.replace("  ", " ")

    dirty = dirty.strip()
    if dirty[-1] == ",":
        dirty = dirty[:-1]
    return dirty.strip()


def find_in_list(value: str, items: []) -> int:
    for i in range(len(items)):
        if items[i] == value:
            return i
    return -1


def get_diff_list(old_list: list[SchemaObject], new_list: list[SchemaObject]) -> list[SchemaObject]:
    old_names = [f.name for f in old_list]
    new_names = [f.name for f in new_list]

    # get new items
    new_names = [f for f in new_names if f not in old_names]

    # get deleted items
    deleted_names = [f for f in old_names if f not in new_names]

    # get modified items
    modified_items = [f.set_operation(OperationType.Modify) for f in old_list if f not in new_list]

    new_items = [f.set_operation(OperationType.Create) for f in new_list if f.name in new_names]
    deleted_items = [f.set_operation(OperationType.Drop) for f in old_list if f.name in deleted_names]

    return modified_items + new_items + deleted_items
