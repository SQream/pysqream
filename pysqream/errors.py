"""Provide all error classes hierarchy for pysqream

Errors conform to python DB API

Python has built-in Warning, so it is not necessary to add new class
"""


class Error(Exception):
    """Exception that is the base class of all other error exceptions.

    Could be used to catch all errors with one single except statement.
    """


class InterfaceError(Error):
    """Raised for errors that are related to the database interface rather
    than the database itself.
    """


class DatabaseError(Error):
    """Exception raised for errors that are related to the database."""


class DataError(DatabaseError):
    """Raised for errors that are due to problems with the processed data

    E.g. division by zero, numeric value out of range, etc.
    """


class OperationalError(DatabaseError):
    """Raised for errors that are related to the database’s operation

    Not necessarily under the control of the programmer, e.g. an
    unexpected disconnect occurs, the data source name is not found, a
    transaction could not be processed, a memory allocation error
    occurred during processing, etc.
    """


class IntegrityError(DatabaseError):
    """Raised when the relational integrity of the database is affected

    E.g. a foreign key check fails.
    """


class InternalError(DatabaseError):
    """Raised when the database encounters an internal error

    E.g. the cursor is not valid anymore, the transaction is out of
    sync, etc.
    """


class ProgrammingError(DatabaseError):
    """Raised for programming errors

    E.g. table not found or already exists, syntax error in the SQL
    statement, wrong number of parameters specified, etc.
    """


class NotSupportedError(DatabaseError):
    """
    Raised in case a method or database API was used which is not supported
    by the database

    E.g. requesting a .rollback() on a connection that does not support
    transaction or has transactions turned off.
    """


class ArraysAreDisabled(DatabaseError):
    """
    Raised on attempt to use ARRAY while they are disabled for connection
    """


class NonSSLPortError(OperationalError):
    """Raised on SSL errors"""

    def __init__(self, message: str = "Using use_ssl=True but connected to non"
                                      " ssl sqreamd port"):
        super().__init__(message)
