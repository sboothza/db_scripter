import re

from pymssql.exceptions import DataError
from sb_serializer import Naming

from adaptor import Adaptor
from mssql_adaptor import MsSqlAdaptor
# from mysql_adaptor import MySqlAdaptor
# from pgsql_adaptor import PgSqlAdaptor
# from sqlite_adaptor import SqliteAdaptor


class AdaptorFactory(object):
    @classmethod
    def get_adaptor_for_connection_string(cls, connection_string: str, naming: Naming) -> Adaptor:
        match = re.search(r"(\w+):\/\/(.+)", connection_string)
        if match:
            db_type = match.group(1)
            connection = match.group(2)

            if db_type == "sqlite":
                raise DataError("Not supported yet")
                # return SqliteAdaptor(connection_string, naming)
            elif db_type == "mysql":
                raise DataError("Not supported yet")
                # return MySqlAdaptor(connection_string, naming)
            elif db_type == "pgsql":
                raise DataError("Not supported yet")
                # return PgSqlAdaptor(connection_string, naming)
            elif db_type == "mssql":
                return MsSqlAdaptor(connection_string, naming)

    @classmethod
    def get_adaptor_for_dbtype(cls, dbtype: str, naming: Naming) -> Adaptor:
        if dbtype.lower() == "sqlite":
            raise DataError("Not supported yet")
            # return SqliteAdaptor("memory", naming)
        elif dbtype.lower() == "mysql":
            raise DataError("Not supported yet")
            # return MySqlAdaptor(MySqlAdaptor.__blank_connection__, naming)
        elif dbtype.lower() == "pgsql":
            raise DataError("Not supported yet")
            # return PgSqlAdaptor(PgSqlAdaptor.__blank_connection__, naming)
        elif dbtype.lower() == "mssql":
            return MsSqlAdaptor(MsSqlAdaptor.__blank_connection__, naming)
