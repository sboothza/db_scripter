import os.path
import shutil
from pathlib import Path

from sb_serializer import Naming, HardSerializer

from src.db_scripter.config import DICTIONARY_FILENAME, BIG_DICTIONARY_FILENAME

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
