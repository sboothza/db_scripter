import os.path
import shutil
from pathlib import Path

from sb_serializer import Naming, HardSerializer

from src.db_scripter.config import DICTIONARY_FILENAME, BIG_DICTIONARY_FILENAME

naming = Naming(DICTIONARY_FILENAME, BIG_DICTIONARY_FILENAME)
serializer = HardSerializer(naming=naming)

