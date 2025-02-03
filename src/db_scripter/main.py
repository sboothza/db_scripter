from sb_serializer import Naming

from src.db_scripter.adaptor_factory import AdaptorFactory


def main():
    naming = Naming("../../dictionary.txt", "../../bigworddictionary.txt")
    adaptor = AdaptorFactory.get_adaptor_for_connection_string("mssql://sa:E15ag0at123@localhost/Polly_C", naming)
    db = adaptor.import_schema("Polly_C")


if __name__ == "__main__":
    main()