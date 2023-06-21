"""Python connector / adapter for SQream DB

The Python connector for SQream DB is a Python DB API 2.0-compliant
interface for developing Python applications with SQream DB.

The SQream Python connector provides an interface for creating and
running Python applications that can connect to a SQream DB database.
It provides a lighter-weight alternative to working through native C++
or Java bindings, including JDBC and ODBC drivers.
"""
# Avoid adding the entire folder to sys.path.
# Importing modules directly in files can be confusing.
# For example, in connection.py, instead of using "from ping import ...",
# use a relative import like this:
# from .ping import ...
from .pysqream import connect, __version__, enable_logs, stop_logs

__all__ = [
    "connect",
    "__version__",
    "enable_logs",
    "stop_logs",
]
