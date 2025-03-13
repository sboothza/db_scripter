import argparse

from adaptor_factory import AdaptorFactory
from common import serializer
from database_objects import Database
from options import Options
from src.db_scripter.config import EXCLUDE


def main():
    parser = argparse.ArgumentParser(description="DB Scripter")
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
                        choices=['import-schema', 'export-schema', 'diff-schema'])

    args = parser.parse_args()
    adaptor = AdaptorFactory.get_adaptor_for_connection_string(args.connection_string)

    options = Options()
    if "tables" in EXCLUDE:
        options["exclude-tables"] = "True"
    if "views" in EXCLUDE:
        options["exclude-views"] = "True"
    if "functions" in EXCLUDE:
        options["exclude-functions"] = "True"
    if "udts" in EXCLUDE:
        options["exclude-udts"] = "True"
    if "storedprocedures" in EXCLUDE:
        options["exclude-storedprocedures"] = "True"
    if "foreignkeys" in EXCLUDE:
        options["exclude-foreignkeys"] = "True"
    if "constraints" in EXCLUDE:
        options["exclude-constraints"] = "True"
    if "primarykeys" in EXCLUDE:
        options["exclude-primarykeys"] = "True"
    if "dependencies" in EXCLUDE:
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

    elif args.operation == "diff-schema":
        with open(args.schema_file, "r", 1024, encoding="utf8") as f:
            json = f.read()

        db_old: Database = serializer.de_serialize(json, Database)
        db_new: Database = adaptor.import_schema(options=options)
        db_diff = db_old.get_diff(db_new)

        adaptor.write_schema(db_diff, args.schema_location)


if __name__ == "__main__":
    main()
