import os.path
from pathlib import Path


def create_dir(path: str) -> str:
    if not os.path.exists(path):
        os.mkdir(path)


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
