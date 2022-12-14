"""SQream Native Python API"""


from datetime import datetime, date, time as t
import time
from pysqream.globals import __version__
from pysqream.logger import log_and_raise, start_logging, stop_logging
from pysqream.connection import Connection


def enable_logs(log_path=None):
    start_logging(None if log_path is True else log_path)


def stop_logs():
    stop_logging()


def connect(host, port, database, username, password, clustered=False,
            use_ssl=False, service='sqream', log=False, reconnect_attempts=3, reconnect_interval=10):
    ''' Connect to SQream database '''
    if not isinstance(reconnect_attempts, int) or reconnect_attempts < 0:
        log_and_raise(Exception, f'reconnect attempts should be a positive integer, got : {reconnect_attempts}')
    if not isinstance(reconnect_interval, int) or reconnect_attempts < 0:
        log_and_raise(Exception, f'reconnect interval should be a positive integer, got : {reconnect_interval}')

    conn = Connection(host, port, clustered, use_ssl, log=log, base_connection=True,
                        reconnect_attempts=reconnect_attempts, reconnect_interval=reconnect_interval)
    conn.connect_database(database, username, password, service)

    return conn


## DBapi compatibility
#  -------------------
''' To fully comply to Python's DB-API 2.0 database standard. Ignore when using internally '''

# Type objects and constructors required by the DB-API 2.0 standard
Binary = memoryview
Date = date
Time = t
Timestamp = datetime


class _DBAPITypeObject:
    """DB-API type object which compares equal to all values passed to the constructor.
        https://www.python.org/dev/peps/pep-0249/#implementation-hints-for-module-authors
    """
    def __init__(self, *values):
        self.values = values

    def __eq__(self, other):
        return other in self.values


STRING = "STRING"
BINARY = _DBAPITypeObject("BYTES", "RECORD", "STRUCT")
NUMBER = _DBAPITypeObject("INTEGER", "INT64", "FLOAT", "FLOAT64", "NUMERIC",
                          "BOOLEAN", "BOOL")
DATETIME = _DBAPITypeObject("TIMESTAMP", "DATE", "TIME", "DATETIME")
ROWID = "ROWID"

typecodes = {
    'ftBool': 'NUMBER',
    'ftUByte': 'NUMBER',
    'ftInt': 'NUMBER',
    'ftShort': 'NUMBER',
    'ftLong': 'NUMBER',
    'ftDouble': 'NUMBER',
    'ftFloat': 'NUMBER',
    'ftDate': 'DATETIME',
    'ftDateTime': 'DATETIME',
    'ftVarchar': 'STRING',
    'ftBlob': 'STRING',
    'ftNumeric': 'NUMBER'
}


def DateFromTicks(ticks):
    return Date.fromtimestamp(ticks)


def TimeFromTicks(ticks):
    return Time(
        *time.localtime(ticks)[3:6]
    )  # localtime() returns a namedtuple, fields 3-5 are hr/min/sec


def TimestampFromTicks(ticks):
    return Timestamp.fromtimestamp(ticks)


# DB-API global parameters
apilevel = '2.0' 

threadsafety = 1 # Threads can share the module but not a connection

paramstyle = 'qmark'


# if __name__ == '__main__':
#
#     print('PySqream DB-API connector, version ', __version__)
#
#     conn = connect("192.168.0.35", 5000, "master", "sqream", "sqream")
    # cur = conn.cursor()
    # cur.execute("select 1")
    # res = cur.fetchall()
    # print(res)
    # conn.close()