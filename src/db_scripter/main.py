import argparse

import orjson
from sb_serializer import Naming, HardSerializer

from adaptor_factory import AdaptorFactory
from database_objects import Database
from src.db_scripter.common import naming, serializer
from src.db_scripter.options import Options


def main():
    parser = argparse.ArgumentParser(description="DB Scripter")
    # parser.add_argument('--dictionary-file',
    #                     help='Path to dictionary file',
    #                     dest='dictionary_file',
    #                     required=True)
    # parser.add_argument('--big-dictionary-file',
    #                     help='Path to big dictionary file',
    #                     dest='big_dictionary_file',
    #                     required=True)
    parser.add_argument('--connection-string',
                        help='DB Connection String',
                        dest='connection_string')
    parser.add_argument('--schema-file',
                        help='Schema file',
                        dest='schema_file')
    parser.add_argument('--schema-location',
                        help='Schema location',
                        dest='schema_location')
    parser.add_argument('--operation',
                        help='Operation',
                        type=str.lower,
                        required=True,
                        choices=['import-schema', 'export-schema'])
    parser.add_argument('--exclude',
                        help='Db object types to exclude (tables,views,functions,udts,storedprocedures,foreignkeys,constraints,primarykeys,dependencies',
                        type=str.lower,
                        default='')

    args = parser.parse_args()

    # naming = Naming(args.dictionary_file, args.big_dictionary_file)
    # serializer = HardSerializer(naming=naming)

    adaptor = AdaptorFactory.get_adaptor_for_connection_string(args.connection_string)
    # adaptor = AdaptorFactory.get_adaptor_for_connection_string("mssql://DV4-POLSQLAG-01/Polly_C?integrated_authentication=True", naming)

    options = Options()
    if "tables" in args.exclude:
        options["exclude-tables"] = "True"
    if "views" in args.exclude:
        options["exclude-views"] = "True"
    if "functions" in args.exclude:
        options["exclude-functions"] = "True"
    if "udts" in args.exclude:
        options["exclude-udts"] = "True"
    if "storedprocedures" in args.exclude:
        options["exclude-storedprocedures"] = "True"
    if "foreignkeys" in args.exclude:
        options["exclude-foreignkeys"] = "True"
    if "constraints" in args.exclude:
        options["exclude-constraints"] = "True"
    if "primarykeys" in args.exclude:
        options["exclude-primarykeys"] = "True"
    if "dependencies" in args.exclude:
        options["exclude-dependencies"] = "True"

    if args.operation == "import-schema":
        db = adaptor.import_schema(options=options)

        json = serializer.serialize(db, True)

        with open(args.schema_file, "w", 1024, encoding="utf8") as f:
            f.write(json)
            f.flush()

    elif args.operation == "export-schema":
        with open(args.schema_file, "r", 1024, encoding="utf8") as f:
            json = f.read()


        db = serializer.de_serialize(json, Database)

        adaptor.write_schema(db, args.schema_location)


if __name__ == "__main__":
    main()
