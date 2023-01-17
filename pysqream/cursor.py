from pysqream.utils import version_compare
import json
from pysqream.globals import BUFFER_SIZE, ROWS_PER_FLUSH, DEFAULT_CHUNKSIZE, FETCH_MANY_DEFAULT, typecodes, type_to_letter, \
    ARROW, pa, csv
from pysqream.column_buffer import ColumnBuffer
from pysqream.ping import PingLoop, _start_ping_loop, _end_ping_loop
from pysqream.logger import *
from pysqream.utils import NotSupportedError, ProgrammingError, InternalError, IntegrityError, OperationalError, DataError, \
    DatabaseError, InterfaceError, Warning, Error
from pysqream.casting import lengths_to_pairs, sq_date_to_py_date, sq_datetime_to_py_datetime, sq_numeric_to_decimal
from pysqream.SQSocket import Client
import time


class Cursor:

    def __init__(self, conn, cursors):

        self.conn = conn
        self.cursors = cursors
        self.s = self.conn.s
        self.client = Client(self.s)
        self.version = self.conn.version
        self.open_statement = False
        self.closed = False
        self.buffer = ColumnBuffer(BUFFER_SIZE)  # flushing buffer every BUFFER_SIZE bytes
        self.ping_loop = None
        self.stmt_id = None  # For error handling when called out of order
        self.statement_type = None
        self.arraysize = FETCH_MANY_DEFAULT
        self.rowcount = -1  # DB-API property
        self.more_to_fetch = False
        self.parsed_rows = []
        self.row_size = 0
        self.rows_per_flush = 0
        self.lastrowid = None
        self.base_connection_closed = False
    
    def get_statement_type(self):

        return self.statement_type

    def get_statement_id(self):

        return self.stmt_id

    def _execute_sqream_statement(self, stmt):
        self.latest_stmt = stmt

        if self.open_statement:
            self.close_stmt()
        self.open_statement = True

        self.more_to_fetch = True

        self.stmt_id = json.loads(self.client.send_string('{"getStatementId" : "getStatementId"}'))["statementId"]
        comp = version_compare(self.version, "2020.3.1")
        if (comp is not None and comp > -1):
            self.ping_loop = _start_ping_loop(self.conn)
        stmt_json = json.dumps({"prepareStatement": stmt, "chunkSize": DEFAULT_CHUNKSIZE})
        res = self.client.send_string(stmt_json)

        self.client.validate_response(res, "statementPrepared")
        self.lb_params = json.loads(res)
        if self.lb_params.get('reconnect'):  # Reconnect exists and issued, otherwise False / None

            # Close socket, open a new socket with new port/ip sent be the reconnect response
            self.s.reconnect(
                self.lb_params['ip'], self.lb_params['port_ssl']
                if self.conn.use_ssl else self.lb_params['port'])

            # Send reconnect and reconstruct messages
            reconnect_str = '{{"service": "{}", "reconnectDatabase":"{}", "connectionId":{}, "listenerId":{},"username":"{}", "password":"{}"}}'.format(
                self.conn.service, self.conn.database, self.conn.connection_id,
                self.lb_params['listener_id'], self.conn.username, self.conn.password)
            self.client.send_string(reconnect_str)
            self.client.send_string('{{"reconstructStatement": {}}}'.format(
                self.stmt_id))

        # Reconnected/reconstructed if needed,  send  execute command
        self.client.validate_response(self.client.send_string('{"execute" : "execute"}'), 'executed')

        # Send queryType message/s
        res = json.loads(self.client.send_string('{"queryTypeIn": "queryTypeIn"}'))
        self.column_list = res.get('queryType', '')

        if not self.column_list:
            res = json.loads(
                self.client.send_string('{"queryTypeOut" : "queryTypeOut"}'))
            self.column_list = res.get('queryTypeNamed', '')
            if not self.column_list:
                self.statement_type = 'DML'
                self.close_stmt()
                return

            self.statement_type = 'SELECT' if self.column_list else 'DML'
            self.result_rows = []
            self.unparsed_row_amount = 0
            self.data_columns = []
        else:
            self.statement_type = 'INSERT'

        # {"isTrueVarChar":false,"nullable":true,"type":["ftInt",4,0]}
        self.col_names, self.col_tvc, self.col_nul, self.col_type_tups = \
            list(zip(*[(col.get("name", ""), col["isTrueVarChar"], col["nullable"], col["type"]) for col in self.column_list]))
        self.col_names_map = {
            name: idx
            for idx, name in enumerate(self.col_names)
        }

        if self.statement_type == 'INSERT':
            self.col_types = [type_tup[0] for type_tup in self.col_type_tups]
            self.col_sizes = [type_tup[1] for type_tup in self.col_type_tups]
            self.col_scales = [type_tup[2] for type_tup in self.col_type_tups]
            self.row_size = sum(self.col_sizes) + sum(
                self.col_nul) + 4 * sum(self.col_tvc)
            self.rows_per_flush = ROWS_PER_FLUSH
            self.buffer.init_buffers(self.col_sizes, self.col_nul)

        # if self.statement_type == 'SELECT':
        self.parsed_rows = []
        self.parsed_row_amount = 0

        if self.ping_loop is not None:
            _end_ping_loop(self.ping_loop)
        if logger.isEnabledFor(logging.INFO):
            logger.info \
                (f'Executing statement over connection {self.conn.connection_id} with statement id {self.stmt_id}:\n{stmt}')

    def _fill_description(self):
        """Getting parameters for the cursor's 'description' attribute, even for
           a query that returns no rows. For each column, this includes:
           (name, type_code, display_size, internal_size, precision, scale, null_ok)"""

        if self.statement_type != 'SELECT':
            self.description = None
            return self.description

        self.description = []
        for col_name, col_nullalbe, col_type_tup in zip(
                self.col_names, self.col_nul, self.col_type_tups):
            type_code = typecodes[
                col_type_tup[0]]  # Convert SQream type to DBAPI identifier
            display_size = internal_size = col_type_tup[
                1]  # Check if other size is available from API
            precision = 38
            scale = col_type_tup[2]

            self.description.append(
                (col_name, type_code, display_size, internal_size, precision,
                 scale, col_nullalbe))

        return self.description

    def _send_columns(self, cols=None, capacity=None):
        """Perform network insert - "put" json, header, binarized columns. Used by executemany()"""

        cols = cols or self.cols
        cols = cols if isinstance(cols, (list, tuple, set, dict)) else list(cols)

        capacity = capacity or self.capacity

        # Send columns and metadata to be packed into our buffer
        packed_cols = self.buffer.pack_columns(cols, capacity, self.col_types,
                                               self.col_sizes, self.col_nul,
                                               self.col_tvc, self.col_scales)
        del cols
        byte_count = sum(len(packed_col) for packed_col in packed_cols)

        # Sending put message and binary header
        self.client.send_string(f'{{"put":{capacity}}}', False)
        self.s.send((self.client.generate_message_header(byte_count, False)))

        # Sending packed data (binary buffer)
        for packed_col in packed_cols:
            self.s.send((packed_col))

        self.client.validate_response(self.client.get_response(), '{"putted":"putted"}')
        del packed_cols

    def _fetch(self, sock=None):

        sock = sock or self.s
        # JSON correspondence
        res = self.client.send_string('{"fetch" : "fetch"}')
        self.client.validate_response(res, "colSzs")
        fetch_meta = json.loads(res)
        num_rows_fetched, column_sizes = fetch_meta['rows'], fetch_meta['colSzs']
        if num_rows_fetched == 0:
            self.close_stmt()
            return num_rows_fetched

        # Get preceding header
        self.client.receive(10)

        # Get data as memoryviews of bytearrays.
        unsorted_data_columns = [memoryview(self.client.receive(size)) for idx, size in enumerate(column_sizes)]

        # Sort by columns, taking a memoryview and casting to the proper type
        self.data_columns = []

        for type_tup, nullable, tvc in zip(self.col_type_tups, self.col_nul,
                                           self.col_tvc):
            column = []
            if nullable:
                column.append(unsorted_data_columns.pop(0))
            if tvc:
                column.append(unsorted_data_columns.pop(0).cast('i'))

            column.append(unsorted_data_columns.pop(0))
            if type_tup[0] not in ('ftVarchar', 'ftBlob', 'ftNumeric'):
                column[-1] = column[-1].cast(type_to_letter[type_tup[0]])
            else:
                column[-1] = column[-1].tobytes()
            self.data_columns.append(column)

        self.unparsed_row_amount = num_rows_fetched

        return num_rows_fetched

    def _parse_fetched_cols(self, queue=None):
        """Used by _fetch_and_parse"""

        self.extracted_cols = []

        if not self.data_columns:
            return self.extracted_cols

        for idx, raw_col_data in enumerate(self.data_columns):
            # Extract data according to column type
            if self.col_tvc[idx]:  # nvarchar
                nvarc_sizes = raw_col_data[1 if self.col_nul[idx] else 0]
                col = [
                    raw_col_data[-1][start:end].decode('utf8')
                    for (start, end) in lengths_to_pairs(nvarc_sizes)
                ]
            elif self.col_type_tups[idx][0] == "ftVarchar":
                varchar_size = self.col_type_tups[idx][1]
                col = [
                    raw_col_data[-1][idx:idx + varchar_size].decode(
                        self.conn.varchar_enc, "ignore").replace('\x00', '').rstrip()
                    for idx in range(0, len(raw_col_data[-1]), varchar_size)
                ]
            elif self.col_type_tups[idx][0] == "ftDate":
                col = [sq_date_to_py_date(d) for d in raw_col_data[-1]]
            elif self.col_type_tups[idx][0] == "ftDateTime":
                col = [sq_datetime_to_py_datetime(d) for d in raw_col_data[-1]]
            elif self.col_type_tups[idx][0] == "ftNumeric":
                scale = self.col_type_tups[idx][2]
                col = [
                    # sq_numeric_to_decimal(bytes_to_bigint(raw_col_data[-1][idx:idx + 16]), scale)
                    sq_numeric_to_decimal(raw_col_data[-1][idx:idx + 16], scale)
                    for idx in range(0, len(raw_col_data[-1]), 16)
                ]

            else:
                col = raw_col_data[-1]

            # Fill Nones if / where needed
            if self.col_nul[idx]:
                nulls = raw_col_data[0]  # .tolist()
                col = [
                    item if not null else None
                    for item, null in zip(col, nulls)
                ]
            else:
                pass

            self.extracted_cols.append(col)

        # Done with the raw data buffers
        self.unparsed_row_amount = 0
        self.data_columns = []

        return self.extracted_cols

    def _fetch_and_parse(self, requested_row_amount, data_as='rows'):
        ''' See if this amount of data is available or a fetch from sqream is required
            -1 - fetch all available data. Used by fetchmany() '''

        if data_as == 'rows':
            while (requested_row_amount > len(self.parsed_rows)
                   or requested_row_amount == -1) and self.more_to_fetch:
                self.more_to_fetch = bool(self._fetch())  # _fetch() updates self.unparsed_row_amount

                self.parsed_rows.extend(zip(*self._parse_fetched_cols()))

    def execute(self, query, params=None):
        """Execute a statement. Parameters are not supported"""

        if self.base_connection_closed:
            self.conn._verify_con_open()
        else:
            self.conn._verify_cur_open()
        if params:

            log_and_raise(ProgrammingError, "Parametered queries not supported. \
                If this is an insert query, use executemany() with the data rows as the parameter")

        else:
            self._execute_sqream_statement(query)

        self._fill_description()
        self.rows_fetched = 0
        self.rows_returned = 0
        return self

    def executemany(self, query, rows_or_cols=None, data_as='rows', amount=None):
        ''' Execute a statement, including parametered data insert '''

        # self._verify_open()
        self.execute(query)

        if rows_or_cols is None:
            return self

        if data_as =='alchemy_flat_list':
            # Unflatten SQLalchemy data list
            row_len = len(self.column_list)
            rows_or_cols = [rows_or_cols[i: i +row_len] for i in range(0, len(rows_or_cols), row_len)]
            data_as = 'rows'

        if 'numpy' in repr(type(rows_or_cols[0])):
            data_as = 'numpy'

        # Network insert starts here if data was passed
        column_lengths = [len(row_or_col) for row_or_col in rows_or_cols]
        if column_lengths.count(column_lengths[0]) != len(column_lengths):
            log_and_raise(ProgrammingError,
                          "Incosistent data sequences passed for inserting. Please use rows/columns of consistent length"
                          )
        if data_as == 'rows':
            self.capacity = amount or len(rows_or_cols)
            self.cols = list(zip(*rows_or_cols))
        else:
            self.cols = rows_or_cols
            self.capacity = len(self.cols)

        # Slice a chunk of columns and pass to _send_columns()
        start_idx = 0
        while self.cols != [()]:
            col_chunk = [col[start_idx:start_idx + self.rows_per_flush] for col in self.cols]
            chunk_len = len(col_chunk[0])
            if chunk_len == 0:
                break
            self._send_columns(col_chunk, chunk_len)
            start_idx += self.rows_per_flush
            del col_chunk

            if logger.isEnabledFor(logging.INFO):
                logger.info(f'Sent {chunk_len} rows of data')

        self.close_stmt()

        return self

    def fetchmany(self, size=None, data_as='rows', fetchone=False):
        ''' Fetch an amount of result rows '''

        size = size or self.arraysize
        # self._verify_open()
        if self.statement_type not in (None, 'SELECT'):
            log_and_raise(ProgrammingError ,'No open statement while attempting fetch operation')

        if self.more_to_fetch is False:
            # All data from server for this select statement was fetched
            if len(self.parsed_rows) == 0:
                # Nothing
                return [] if not fetchone else None
        else:
            self._fetch_and_parse(size, data_as)

        # Get relevant part of parsed rows and reduce storage and counter
        if data_as == 'rows':
            res = self.parsed_rows[0:size if size != -1 else None]
            del self.parsed_rows[:size if size != -1 else None]

        if logger.isEnabledFor(logging.INFO):
            logger.info(f'Fetched {size} rows')

        return (res if res else []) if not fetchone else (res[0] if res else None)

    def fetchone(self, data_as='rows'):
        """Fetch one result row"""

        if data_as not in ('rows',):
            log_and_raise(ProgrammingError, "Bad argument to fetchone()")

        return self.fetchmany(1, data_as, fetchone=True)

    def fetchall(self, data_as='rows'):
        """Fetch all result rows"""

        if data_as not in ('rows',):
            log_and_raise(ProgrammingError, "Bad argument to fetchall()")

        return self.fetchmany(-1, data_as)

    # DB-API Do nothing (for now) methods
    # -----------------------------------

    def nextset(self):
        """No multiple result sets so currently always returns None"""
        log_and_raise(NotSupportedError, "Nextset is not supported")

    def setinputsizes(self, sizes):
        log_and_raise(NotSupportedError, "Setinputsizes is not supported")

    def setoutputsize(self, size, column=None):
        log_and_raise(NotSupportedError, "Setoutputsize is not supported")

    # def csv_to_table(self, csv_path, table_name, read=None, parse=None, convert=None, con=None, auto_infer=False,
    #                  delimiter="|"):
    #     ' Pyarrow CSV reader documentation: https://arrow.apache.org/docs/python/generated/pyarrow.csv.read_csv.html '
    #
    #     if not ARROW:
    #         return "Optional pyarrow dependency not found. To install: pip3 install pyarrow"
    #
    #     sqream_to_pa = {
    #         'ftBool':     pa.bool_(),
    #         'ftUByte':    pa.uint8(),
    #         'ftShort':    pa.int16(),
    #         'ftInt':      pa.int32(),
    #         'ftLong':     pa.int64(),
    #         'ftFloat':    pa.float32(),
    #         'ftDouble':   pa.float64(),
    #         'ftDate':     pa.timestamp('ns'),
    #         'ftDateTime': pa.timestamp('ns'),
    #         'ftVarchar':  pa.string(),
    #         'ftBlob':     pa.utf8(),
    #         'ftNumeric':  pa.decimal128(28, 11)
    #     }
    #
    #     start = time.time()
    #     # Get table metadata
    #     self.execute(f'select * from {table_name} where 1=0')
    #
    #     # Map column names to pyarrow types and set Arrow's CSV parameters
    #     sqream_col_types = [col_type[0] for col_type in self.col_type_tups]
    #     column_types = zip(self.col_names, [sqream_to_pa[col_type[0]] for col_type in self.col_type_tups])
    #     read = read or csv.ReadOptions(column_names=self.col_names)
    #     parse = parse or csv.ParseOptions(delimiter=delimiter)
    #     convert = convert or csv.ConvertOptions(column_types=None if auto_infer else column_types,
    #                                             null_values=["\\N"])
    #
    #     # Read CSV to in-memory arrow format
    #     csv_arrow = csv.read_csv(csv_path, read_options=read, parse_options=parse, convert_options=convert).\
    #         combine_chunks()
    #     num_chunks = len(csv_arrow[0].chunks)
    #     numpy_cols = []
    #
    #     # For each column, get the numpy representation for quick packing
    #     for col_type, col in zip(sqream_col_types, csv_arrow):
    #         # Only one chunk after combine_chunks()
    #         col = col.chunks[0]
    #         if col_type in  ('ftVarchar', 'ftBlob', 'ftDate', 'ftDateTime', 'ftNumeric'):
    #             col = col.to_pandas()
    #         else:
    #             col = col.to_numpy()
    #
    #         numpy_cols.append(col)
    #
    #     print(f'total loading csv: {time.time( ) -start}')
    #     start = time.time()
    #
    #     # Insert columns into SQream
    #     col_num = csv_arrow.shape[1]
    #     self.executemany(f'insert into {table_name} values ({"?, " *(col_num-1)}?)', numpy_cols)
    #     print(f'total inserting csv: {time.time( ) -start}')

    ## Closing

    def close_stmt(self):
        if self.open_statement:
            self.client.send_string('{"closeStatement": "closeStatement"}')
            self.open_statement = False

            if logger.isEnabledFor(logging.INFO):
                logger.info(f'Done executing statement {self.stmt_id} over connection {self.conn.connection_id}')

    def close(self, sock=None):
        self.close_stmt()
        sock = sock or self.s
        self.conn.cur_closed = True
        self.conn.close_connection()
        self.closed = True
        self.buffer.close()
        _end_ping_loop(self.ping_loop)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.close()

    def __iter__(self):
        for item in self.fetchall():
            yield item