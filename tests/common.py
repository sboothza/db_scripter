from sb_serializer import Naming, HardSerializer
from config import DICTIONARY_FILENAME, BIG_DICTIONARY_FILENAME

naming = Naming(DICTIONARY_FILENAME, BIG_DICTIONARY_FILENAME)
serializer = HardSerializer(naming=naming)

