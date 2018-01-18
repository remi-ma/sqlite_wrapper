##############################################################################################
# Project     : sqlite3 wrapper
# File        : sql_exception.py
# Author      : Remi Malaquin
# Date        : 01/17/2018
# Description : Just declare Exception here
##############################################################################################


# Exceptions
class SqlTypeParameterError(Exception):
    pass


class SqlNameParameterError(Exception):
    pass


class SqlLengthParameterError(Exception):
    pass


class InsertionKeyNotFoundError(Exception):
    pass


class SqlDoubleItemsOccurs(Exception):
    pass


class SqlMissingPrimaryKey(Exception):
    pass


class SqlNoElementFound(Exception):
    pass


class SqlTableUnknown(Exception):
    pass


class SqlTableNameEmptyError(Exception):
    pass


class SqlFilterKeyEmptyError(Exception):
    pass


class SqlExistOnDestinationTableError(Exception):
    pass


class SqlSeveralElementItemsSelected(Exception):
    pass


class SqlTablesAreDifferentsError(Exception):
    pass


class SqlTableAlreadyExistError(Exception):
    pass


class SqlNoItemToMoveError(Exception):
    pass

# Warnings


class SqlCreateTableWarning(Warning):
    pass
