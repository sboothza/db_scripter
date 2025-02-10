import os

import sb_serializer
from sb_serializer import Naming, HardSerializer

from src.db_scripter.adaptor_factory import AdaptorFactory


def main():
    naming = Naming("../../dictionary.txt", "../../bigworddictionary.txt")
    adaptor = AdaptorFactory.get_adaptor_for_connection_string("mssql://sa:E15ag0at123@localhost/AdventureWorks", naming)
    # adaptor = AdaptorFactory.get_adaptor_for_connection_string("mssql://DV4-POLSQLAG-01/Polly_C?integrated_authentication=True", naming)
    db = adaptor.import_schema("AdventureWorks")

    # db.trim_db(50)

    # serializer = HardSerializer(naming=naming)

    # json = serializer.serialize(db, True)
    #
    # print(json)
    # with open("./db.json", "w", 1024, encoding="utf8") as f:
    #     f.write(json)
    #     f.flush()


    adaptor.write_schema(db, "S:\\src\\pvt\\db_scripter\\sql")


if __name__ == "__main__":
    main()
