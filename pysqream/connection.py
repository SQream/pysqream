from .column_buffer import ColumnBuffer
from .SQSocket import SQSocket, Client
from .ping import PingLoop
from .globals import BUFFER_SIZE, FETCH_MANY_DEFAULT, CYTHON
from .logger import *
import json
import time
from queue import Queue, Empty
from struct import unpack
import socket
from .casting import date_to_int as pydate_to_int, datetime_to_long as pydt_to_long, sq_date_to_py_date as date_to_py, sq_datetime_to_py_datetime as dt_to_py
from .cursor import Cursor


class Connection:
    ''' Connection class used to interact with SQream '''

    base_conn_open = [False]

    def __init__(self, ip, port, clustered, use_ssl=False, log=False, base_connection=True,
                 reconnect_attempts=3, reconnect_interval=10):

        self.buffer = ColumnBuffer(BUFFER_SIZE)  # flushing buffer every BUFFER_SIZE bytes
        self.row_size = 0
        self.rows_per_flush = 0
        self.version = None
        self.stmt_id = None  # For error handling when called out of order
        self.statement_type = None
        self.open_statement = False
        self.closed = False
        self.opened = False
        self.orig_ip, self.orig_port, self.clustered, self.use_ssl = ip, port, clustered, use_ssl
        self.reconnect_attempts, self.reconnect_interval = reconnect_attempts, reconnect_interval
        self.base_connection = base_connection
        self.ping_loop = None
        self.client = None

        if self.base_connection:
            self.cursors = []

        self._open_connection(clustered, use_ssl)
        self.arraysize = FETCH_MANY_DEFAULT
        self.rowcount = -1    # DB-API property
        self.more_to_fetch = False
        self.parsed_rows = []

        self.lastrowid = None
        self.unpack_q = Queue()
        if CYTHON:
            # To allow hot swapping for testing
            date_to_int, datetime_to_long, sq_date_to_py_date, sq_datetime_to_py_datetime = pydate_to_int, pydt_to_long, date_to_py, dt_to_py

        if log is not False:
            start_logging(None if log is True else log)
        # Thread for unpacking fetched socket data
        # thread.start_new_thread(self._parse_fetched_cols, (self.unpack_q,))

    def __del__(self):
        try:
            self.close()
        except Exception as e:
            if "Trying to close a connection that's already closed" not in repr(e):
                log_and_raise(ProgrammingError, e)

    ## SQream mechanisms

    def _open_connection(self, clustered, use_ssl):
        ''' Get proper ip and port from picker if needed and connect socket. Used at __init__() '''
        if clustered is True:

            # Create non SSL socket for picker communication
            picker_socket = SQSocket(self.orig_ip, self.orig_port, False)
            self.client = Client(picker_socket)
            # Parse picker response to get ip and port
            # Read 4 bytes to find length of how much to read
            picker_socket.timeout(5)
            try:
                read_len = unpack('i', self.client.receive(4))[0]
            except socket.timeout:

                log_and_raise(ProgrammingError, f'Connected with clustered=True, but apparently not a server picker port')
            picker_socket.timeout(None)

            # Read the number of bytes, which is the IP in string format
            # Using a nonblocking socket in case clustered = True was passed but not connected to picker
            self.ip = self.client.receive(read_len)

            # Now read port
            self.port = unpack('i', self.client.receive(4))[0]
            picker_socket.close()
        else:
            self.ip, self.port = self.orig_ip, self.orig_port

        # Create socket and connect to actual SQreamd server
        self.s = SQSocket(self.ip, self.port, use_ssl)
        self.client = Client(self.s)
        if self.base_connection:
            self.base_conn_open[0] = True

    def connect_database(self, database, username, password, service='sqream'):
        ''' Handle connection to database, with or without server picker '''

        self.database, self.username, self.password, self.service = database, username, password, service
        res = self.client.send_string(
            f'{{"username":"{username}", "password":"{password}", "connectDatabase":"{database}", "service":"{service}"}}'
        )
        res = json.loads(res)
        try:
            self.connection_id = res['connectionId']
            if 'version' in res:
                self.version = res['version']
        except KeyError as e:
            log_and_raise(ProgrammingError, f"Error connecting to database: {res['error']}")

        self.varchar_enc = res.get('varcharEncoding', 'ascii')

        if logger.isEnabledFor(logging.INFO):
            logger.info(f'Connection opened to database {database}. Connection ID: {self.connection_id}')
        self.opened = True

    def _attempt_reconnect(self):

        for attempt in range(self.reconnect_attempts):
            time.sleep(self.reconnect_interval)
            try:
                self._open_connection(self.clustered, self.use_ssl)
                self.connect_database(self.database, self.username, self.password, self.service)
            except ConnectionRefusedError as e:
                print(f'Connection lost, retry attempt no. {attempt+1} failed. Original error:\n{e}')
            else:
                return

        # Attempts failed
        log_and_raise(ConnectionRefusedError, 'Reconnection attempts to sqreamd failed')

    def close_connection(self, sock=None):

        if self.closed:
            log_and_raise(ProgrammingError, "Trying to close a connection that's already closed")

        self.client.send_string('{"closeConnection":  "closeConnection"}')
        self.s.close()
        self.buffer.close()
        self._end_ping_loop()
        self.closed = True
        self.base_conn_open[0] = False if self.base_connection else True

        if logger.isEnabledFor(logging.INFO):
            logger.info(f'Connection closed to database {self.database}. Connection ID: {self.connection_id}')

    '''  -- Metadata  --
         ---------------   '''
    def get_statement_type(self):

        return self.statement_type

    def get_statement_id(self):

        return self.stmt_id


    ## DB-API API
    #  ----------

    def _verify_open(self):

        if not self.base_conn_open[0]:
            log_and_raise(ProgrammingError, 'Connection has been closed')

        if self.closed:
            log_and_raise(ProgrammingError, 'Cursor has been closed')

    def _verify_query_type(self, query_type):

        if not self.open_statement:
            log_and_raise(ProgrammingError, 'No open statement while attempting fetch operation')

    def cursor(self):
        ''' Return a new connection with the same parameters.
            We use a connection as the equivalent of a 'cursor' '''

        conn = Connection(
            self.orig_ip if self.clustered is True else self.ip,
            self.orig_port if self.clustered is True else self.port,
            self.clustered,
            self.use_ssl,
            base_connection=False
        )  # self is the calling connection instance, so cursor can trace back to pysqream
        conn.connect_database(self.database, self.username, self.password, self.service)

        self._verify_open()
        cur = Cursor(conn)
        self.cursors.append(cur)
        return cur

    def commit(self):
        self._verify_open()

    def rollback(self):
        pass

    def close(self):

        if self.opened:
            if self.closed:
                log_and_raise(ProgrammingError, "Trying to close a connection that's already closed")

            if self.base_connection:
                for cursor in self.cursors:
                    try:
                        cursor.close()
                    except:
                        pass

            self.close_connection()
            self.closed = True

    def nextset(self):
        ''' No multiple result sets so currently always returns None '''

        return None

    # DB-API Do nothing (for now) methods
    # -----------------------------------

    def setinputsizes(self, sizes):

        self._verify_open()

    def setoutputsize(self, size, column=None):

        self._verify_open()

    # Internal Methods
    # ----------------
    ''' Include: __enter__(), __exit__() - for using "with", 
                 __iter__ for use in for-in clause  '''

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.close()

    def _start_ping_loop(self):
        self.ping_loop = PingLoop(self)
        self.ping_loop.start()

    def _end_ping_loop(self):
        if self.ping_loop is not None:
            self.ping_loop.halt()
            self.ping_loop.join()
        self.ping_loop = None


class Error(Exception):
    pass


class Warning(Exception):
    pass


class InterfaceError(Error):
    pass


class DatabaseError(Error):
    pass


class DataError(DatabaseError):
    pass


class OperationalError(DatabaseError):
    pass


class IntegrityError(DatabaseError):
    pass


class InternalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


class NotSupportedError(DatabaseError):
    pass