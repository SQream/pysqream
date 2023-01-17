import logging

from pysqream.column_buffer import ColumnBuffer
from pysqream.SQSocket import SQSocket, Client
from ping import PingLoop, _end_ping_loop
from pysqream.globals import BUFFER_SIZE, FETCH_MANY_DEFAULT, CYTHON
from pysqream.logger import *
import json
import time
from queue import Queue, Empty
from struct import unpack
import socket
from pysqream.utils import NotSupportedError, ProgrammingError, InternalError, IntegrityError, OperationalError, DataError, \
    DatabaseError, InterfaceError, Warning, Error
from pysqream.casting import date_to_int as pydate_to_int, datetime_to_long as pydt_to_long, sq_date_to_py_date as date_to_py, sq_datetime_to_py_datetime as dt_to_py
from pysqream.cursor import Cursor


class Connection:
    ''' Connection class used to interact with SQream '''

    def __init__(self, ip, port, clustered, use_ssl=False, log=False, base_connection=True,
                 reconnect_attempts=3, reconnect_interval=10):

        self.buffer = ColumnBuffer(BUFFER_SIZE)  # flushing buffer every BUFFER_SIZE bytes
        self.version = None
        self.cur_closed = False
        self.con_closed = False
        self.connect_to_socket = False
        self.connect_to_database = False
        self.orig_ip, self.orig_port, self.clustered, self.use_ssl = ip, port, clustered, use_ssl
        self.reconnect_attempts, self.reconnect_interval = reconnect_attempts, reconnect_interval
        self.base_connection = base_connection
        self.ping_loop = None
        self.client = None
        self.cursors = {}

        self._open_connection(clustered, use_ssl)

        self.unpack_q = Queue()
        if CYTHON:
            # To allow hot swapping for testing
            date_to_int, datetime_to_long, sq_date_to_py_date, sq_datetime_to_py_datetime = pydate_to_int, pydt_to_long, date_to_py, dt_to_py

        if log is not False:
            raise NotSupportedError("Logs per Connection is not supported yet")
            # start_logging(None if log is True else log)
        # Thread for unpacking fetched socket data
        # thread.start_new_thread(self._parse_fetched_cols, (self.unpack_q,))

    def __del__(self):
        try:
            logger.debug("Try to destroy open connections")
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
        self.connect_to_socket = True

    def connect_database(self, database, username, password, service='sqream'):
        """Handle connection to database, with or without server picker"""

        self.database, self.username, self.password, self.service = database, username, password, service
        if self.connect_to_socket:
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
            self.connect_to_database = True

    def _attempt_reconnect(self):

        for attempt in range(self.reconnect_attempts):
            time.sleep(self.reconnect_interval)
            try:
                logger.info(f"attempt to connect numer {attempt}")
                self._open_connection(self.clustered, self.use_ssl)
                self.connect_database(self.database, self.username, self.password, self.service)
            except ConnectionRefusedError as e:
                print(f'Connection lost, retry attempt no. {attempt+1} failed. Original error:\n{e}')
            else:
                return

        # Attempts failed
        log_and_raise(ConnectionRefusedError, 'Reconnection attempts to sqreamd failed')

    def close_connection(self):

        if self.con_closed:
            log_and_raise(ProgrammingError, f"Trying to close a connection that's already closed for database "
                                            f"{self.database} and Connection ID: {self.connection_id}")
        self.client.send_string('{"closeConnection":  "closeConnection"}')
        self.s.close()
        self.buffer.close()
        self.con_closed = True

        if logger.isEnabledFor(logging.INFO):
            logger.info(f'Connection closed to database {self.database}. Connection ID: {self.connection_id}')

    ## DB-API API
    #  ----------

    # def _verify_open(self):
    #
    #     if self.cur_closed:
    #         log_and_raise(ProgrammingError, 'Cursor has been closed')
    #
    #     if self.con_closed:
    #         log_and_raise(ProgrammingError, 'Connection has been closed')

    def _verify_cur_open(self):
        if self.cur_closed:
            log_and_raise(ProgrammingError, 'Cursor has been closed')

    def _verify_con_open(self):
        if self.con_closed:
            log_and_raise(ProgrammingError, 'Connection has been closed')

    def cursor(self):
        ''' Return a new connection with the same parameters.
            We use a connection as the equivalent of a 'cursor' '''

        logger.debug("Create cursor")
        conn = Connection(
            self.orig_ip if self.clustered is True else self.ip,
            self.orig_port if self.clustered is True else self.port,
            self.clustered,
            self.use_ssl,
            base_connection=False
        )  # self is the calling connection instance, so cursor can trace back to pysqream
        conn.connect_database(self.database, self.username, self.password, self.service)

        self._verify_con_open()
        cur = Cursor(conn, self.cursors)
        self.cursors[cur.conn.connection_id] = cur
        return cur

    def commit(self):
        return
        # log_and_raise(NotSupportedError, "Commit is not supported")

    def rollback(self):
        return
        # log_and_raise(NotSupportedError, "Rollback is not supported")

    def close(self):

        if not self.connect_to_database:
            return

        if self.base_connection:
            for con_id, cursor in self.cursors.items():
                try:
                    if not cursor.closed:
                        cursor.base_connection_closed = True
                        cursor.close()
                except Exception as e:
                    logger.error(f"Can't close connection - {e} for Connection ID {con_id}")
                    raise Error(f"Can't close connection - {e} for Connection ID {con_id}")
            self.cursors = {}

        self.close_connection()


    # Internal Methods
    # ----------------
    ''' Include: __enter__(), __exit__() - for using "with", 
                 __iter__ for use in for-in clause  '''

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.close()