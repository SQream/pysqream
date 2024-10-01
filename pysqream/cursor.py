"""Contain Cursor, which is used to manage the context of a fetch.

Gets statement, send it to SQream then check if statement ready and
send it again for exectuion.
Responsible for both fetching and extracting data.

Should be used only by .connection.Connection
"""
from __future__ import annotations

import functools
import json
import logging
import struct

from typing import List, Any, Union

from pysqream.casting import (lengths_to_pairs,
                              sq_date_to_py_date,
                              sq_datetime_to_py_datetime,
                              sq_numeric_to_decimal,
                              arr_lengths_to_pairs)
from pysqream.column_buffer import ColumnBuffer
from pysqream.globals import (BUFFER_SIZE,
                              ROWS_PER_FLUSH,
                              DEFAULT_CHUNKSIZE,
                              FETCH_MANY_DEFAULT,
                              typecodes,
                              type_to_letter,
                              BYTES_PER_FLUSH_LIMIT,
                              TEXT_ITEM_SIZE,
                              CAN_SUPPORT_PARAMETERS)
from pysqream.logger import log_and_raise, logger, printdbg
from pysqream.ping import _start_ping_loop, _end_ping_loop
from pysqream.utils import (NotSupportedError,
                            ProgrammingError,
                            get_array_size,
                            false_generator,
                            ArraysAreDisabled,
                            OperationalError)


def _is_null(nullable):
    return nullable == 1


class Cursor:
    """
    Represent a database cursor, which is used to manage the context of
    a fetch operation.

    Cursors created from the same connection are not
    isolated, i.e., any changes done to the database by a cursor are
    immediately visible by the other cursors.

    Base PEP 249 – Python Database API Specification v2.0
    https://peps.python.org/pep-0249/#id12
    """

    def __init__(self, conn, cursors):

        self.conn = conn
        self.cursors = cursors
        self.socket = self.conn.socket
        self.client = self.conn.client
        self.version = self.conn.version
        self.connection_id = self.conn.connection_id
        self.open_statement = False
        self.closed = False
        self.buffer = ColumnBuffer(BUFFER_SIZE)  # flushing buffer every BUFFER_SIZE bytes
        self.stmt_id = None  # For error handling when called out of order
        self.statement_type = None
        self.arraysize = FETCH_MANY_DEFAULT
        self.rowcount = -1  # DB-API property
        self.more_to_fetch = False
        self.ping_loop = self.conn.ping_loop
        self.parsed_rows = []
        self.row_size = 0
        self.rows_per_flush = 0
        self.lastrowid = None
        self.base_connection_closed = False
        self.rows_fetched = None
        self.rows_returned = None
        self.cols = []
        self.capacity = 0

    def get_statement_type(self):
        return self.statement_type

    def get_statement_id(self):
        return self.stmt_id

    def _execute_sqream_statement(self,
                                  statement: str,
                                  params: list[Any] | tuple[Any] | None = None,
                                  data_as: str = "rows",
                                  amount: int | None = None
                                  ) -> None:
        """High-level method overview:
        1) statement preparation + reconnect + execute
        2) queryTypeIn and queryTypeOut
        3) if queryTypeIn isn't empty is means statement was parameterized
        4) collect all information about all columns (such a col types, scales, row_size, etc.) and send it to server
        5) if statement was `select` - perform lists for fetching and do 4 step for queryTypeOut
        """

        self.latest_stmt = statement

        if self.open_statement:
            self.close_stmt()
        self.open_statement = True

        self.more_to_fetch = True

        self.stmt_id = json.loads(self.client.send_string('{"getStatementId" : "getStatementId"}'))["statementId"]
        stmt_json = json.dumps({"prepareStatement": statement,
                                "chunkSize": DEFAULT_CHUNKSIZE,
                                "canSupportParams": CAN_SUPPORT_PARAMETERS})
        res = self.client.send_string(stmt_json)

        self.client.validate_response(res, "statementPrepared")
        self.lb_params = json.loads(res)
        if self.lb_params.get('reconnect'):  # Reconnect exists and issued, otherwise False / None

            # Close socket, open a new socket with new port/ip sent be the reconnect response
            self.socket.reconnect(
                self.lb_params['ip'], self.lb_params['port_ssl']
                if self.conn.use_ssl else self.lb_params['port'])

            # Send reconnect and reconstruct messages
            reconnect_str = (f'{{"service": "{self.conn.service}", '
                             f'"reconnectDatabase":"{self.conn.database}", '
                             f'"connectionId":{self.conn.connection_id}, '
                             f'"listenerId":{self.lb_params["listener_id"]}, '
                             f'"username":"{self.conn.username}", '
                             f'"password":"{self.conn.password}"}}')
            self.client.send_string(reconnect_str)
            # Since summer 2024 sqreamd worker could be configured with non-gpu (cpu) instance
            # it raises exception here like `The query requires a GPU-Worker. Ensure the SQream Service has GPU . . .`
            # This exception should be validated here. Otherwise, it will be validated at the next call which provides
            # Unexpected behavior
            self.client.validate_response(self.client.send_string(f'{{"reconstructStatement": {self.stmt_id}}}'),
                                          "statementReconstructed"
                                          )

        # Reconnected/reconstructed if needed,  send  execute command
        self.client.validate_response(self.client.send_string('{"execute" : "execute"}'), 'executed')

        # Send queryType message/s
        query_type_in = json.loads(self.client.send_string('{"queryTypeIn": "queryTypeIn"}'))
        self.column_list = parameterized_columns = query_type_in.get('queryType', [])

        query_type_out = json.loads(self.client.send_string('{"queryTypeOut" : "queryTypeOut"}'))

        if parameterized_columns:
            # Check if arrays are allowed before executing the rest
            if not self._validate_arrays_usage():
                log_and_raise(ArraysAreDisabled, "Arrays are disabled in this connection.")

            self._generate_columns_data_for_parameterized_statement()
            self.col_types = []
            self.col_sizes = []
            self.col_scales = []
            for type_tup in self.col_type_tups:
                is_array = 'ftArray' in type_tup
                offset = 0
                _type = type_tup[0]
                if is_array:
                    # for array other stuff like scale is shifted in type_tup
                    offset = 1
                    # Use tuple for ftArray that will be checked
                    # only in buffer like 'ftArray' in col_type
                    _type = type_tup[0:2]
                self.col_types.append(_type)
                self.col_sizes.append(type_tup[1 + offset] if type_tup[1 + offset] != 0 else TEXT_ITEM_SIZE)
                self.col_scales.append(type_tup[2 + offset])

            self.row_size = sum([sum(self.col_sizes),
                                 sum(null for null in self.col_nul if null is True),
                                 sum(tvc for tvc in self.col_tvc if tvc is True)
                                 ])

            if self.row_size * ROWS_PER_FLUSH <= BYTES_PER_FLUSH_LIMIT:
                self.rows_per_flush = int(ROWS_PER_FLUSH)
            else:
                self.rows_per_flush = int(BYTES_PER_FLUSH_LIMIT / self.row_size)

            self.buffer.clear()

            if data_as == 'alchemy_flat_list':
                # Unflatten SQLalchemy data list
                row_len = len(self.column_list)
                rows_or_cols = [params[i: i + row_len] for i in range(0, len(params), row_len)]
                data_as = 'rows'
            else:
                rows_or_cols = params

            if 'numpy' in repr(type(params[0])):
                data_as = 'numpy'

            # Send columns and parameters
            # row_len = len(self.column_list)
            # rows_or_cols = [params[i: i + row_len] for i in range(0, len(params), row_len)]
            column_lengths = [len(row_or_col) for row_or_col in rows_or_cols]

            if column_lengths.count(column_lengths[0]) != len(column_lengths):
                log_and_raise(ProgrammingError,
                              "Incosistent data sequences passed for inserting. "
                              "Please use rows/columns of consistent length")

            if data_as == 'rows':
                self.capacity = amount or len(rows_or_cols)
                self.cols = list(zip(*rows_or_cols))
            else:
                self.cols = rows_or_cols
                self.capacity = len(self.cols)

            self._send_columns()

        self.column_list = columns_for_output = query_type_out.get('queryTypeNamed', [])
        if columns_for_output:
            # data in `queryTypeOut` means it was a `SELECT` query
            self.statement_type = "SELECT"
            self.result_rows = []
            self.parsed_rows = []
            self.data_columns = []
            self.unparsed_row_amount = self.parsed_row_amount = 0

            self._generate_columns_data_for_parameterized_statement()

        else:
            self.statement_type = "DML"
            self.close_stmt()

        if logger.isEnabledFor(logging.INFO):
            logger.info(f'Executing statement over connection {self.conn.connection_id} '
                        f'with statement id {self.stmt_id}:\n{statement}')
        return

    def _validate_arrays_usage(self) -> bool:
        """
        Checks if the executing statement uses arrays and if they are allowed.

        Returns:
            bool: False if arrays are not allowed by connection, but used in
              statement, True otherwise.
        """
        if not self.conn.allow_array:
            for col in self.column_list:
                if "ftArray" in col["type"]:
                    return False
        return True

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

    def _generate_columns_data_for_parameterized_statement(self) -> None:
        """To use fetch we need to fill all attributes in list below:
        1) col_names - list of column names
        2) col_tvc - list of `isTrueVarChar` values
        3) col_nul - list of nullable column properties
        4) col_type_tups - list with column types description (type, size, scale)
        5) col_names_map - dictionary with col_name and index (from 0) for every column

        Content could be received from `queryTypeIn` or `queryTypeOut` requests
        For parameterized statements (`executemany` also) `queryTypeIn` always returns list of columns data look like:
        { "isTrueVarChar": false, "nullable": true, "type": ["ftInt", 4, 0] }
        """
        self.col_names = [col.get("name", "") for col in self.column_list]
        self.col_tvc = [col["isTrueVarChar"] for col in self.column_list]
        self.col_nul = [col["nullable"] for col in self.column_list]
        self.col_type_tups = [col["type"] for col in self.column_list]
        self.col_names_map = {name: idx for idx, name in enumerate(self.col_names)}

    def _send_columns(self) -> None:
        """Used for parameterized statements.
        After information about all columns was collected by `_execute_sqream_statement` and
        `_generate_columns_data_for_parameterized_statement` methods

        We need to send these data to server to make it inserts parameters via
        slicing a chunk of columns and pass to _send_column_chunk()
        """

        start_idx = 0
        while self.cols != [()]:
            col_chunk = [col[start_idx:start_idx + self.rows_per_flush] for col in self.cols]
            chunk_len = len(col_chunk[0])
            if chunk_len == 0:
                break
            self._send_column_chunk(col_chunk, chunk_len)
            start_idx += self.rows_per_flush
            del col_chunk

            if logger.isEnabledFor(logging.INFO):
                logger.info(f'Sent {chunk_len} rows of data')

    def _send_column_chunk(self, cols=None, capacity=None):
        """Perform network insert - "put" json, header, binarized columns. Used by executemany()"""

        cols = cols or self.cols
        cols = cols if isinstance(cols, (list, tuple, set, dict)) else list(cols)

        capacity = capacity or self.capacity
        # Send columns and metadata to be packed into our buffer
        packed_cols = self.buffer.pack_columns(cols, capacity, self.col_types,
                                               self.col_sizes, self.col_nul,
                                               self.col_tvc, self.col_scales)
        byte_count = functools.reduce(lambda c, n: c + len(n), packed_cols, 0)

        # Stop and start ping is must between sending message to the server, this is part of the protocol.
        _end_ping_loop(self.ping_loop)
        # Sending put message and binary header
        self.client.send_string(f'{{"put":{capacity}}}', False)
        self.ping_loop = _start_ping_loop(self.client, self.socket)

        _end_ping_loop(self.ping_loop)
        self.socket.send((self.client.generate_message_header(byte_count, False)))
        self.ping_loop = _start_ping_loop(self.client, self.socket)

        # Sending packed data (binary buffer)
        _end_ping_loop(self.ping_loop)
        for packed_col in packed_cols:
            printdbg("Packed data sent:", packed_col)
            self.socket.send((packed_col))

        self.client.validate_response(self.client.get_response(), '{"putted":"putted"}')
        self.ping_loop = _start_ping_loop(self.client, self.socket)

    def _fetch(self):

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
            column = {'nullable': False, 'true_nvarchar': False}

            is_array = 'ftArray' in type_tup

            if nullable:
                column['nullable'] = unsorted_data_columns.pop(0)
            if is_array:
                column['array_lengths'] = unsorted_data_columns\
                    .pop(0).cast('i')
            elif tvc:
                column['true_nvarchar'] = unsorted_data_columns\
                    .pop(0).cast('i')

            column['data_column'] = unsorted_data_columns.pop(0)

            if type_tup[0] in ('ftVarchar', 'ftBlob', 'ftNumeric'):
                column['data_column'] = column['data_column'].tobytes()
            elif not is_array:
                column['data_column'] = column['data_column'].cast(
                    type_to_letter[type_tup[0]])

            self.data_columns.append(column)

        self.unparsed_row_amount = num_rows_fetched

        return num_rows_fetched

    def _parse_fetched_cols(self):
        """Used by _fetch_and_parse"""

        self.extracted_cols = []

        if not self.data_columns:
            return self.extracted_cols

        for idx, raw_col_data in enumerate(self.data_columns):

            # Extract data according to column type
            if self.col_type_tups[idx][0] == "ftArray":
                col = self._extract_array(idx, raw_col_data)

            elif self.col_tvc[idx]:  # nvarchar
                col = self._extract_nvarchar(idx, raw_col_data)

            elif self.col_type_tups[idx][0] == "ftVarchar":
                col = self._extract_varchar(idx, raw_col_data)

            elif self.col_type_tups[idx][0] == "ftDate":
                col = self._extract_date(idx, raw_col_data)

            elif self.col_type_tups[idx][0] == "ftDateTime":
                col = self._extract_datetime(idx, raw_col_data)

            elif self.col_type_tups[idx][0] == "ftNumeric":
                col = self._extract_numeric(idx, raw_col_data)
            else:
                col = self._extract_datatype(idx, raw_col_data)

            self.extracted_cols.append(col)

        # Done with the raw data buffers
        self.unparsed_row_amount = 0
        self.data_columns = []

        return self.extracted_cols

    def _fetch_and_parse(self, requested_row_amount, data_as='rows'):
        """See if this amount of data is available or a fetch from sqream is required
        -1 - fetch all available data. Used by fetchmany()
        """

        if data_as == 'rows':
            while (requested_row_amount > len(self.parsed_rows) or requested_row_amount == -1) and self.more_to_fetch:
                self.more_to_fetch = bool(self._fetch())  # _fetch() updates self.unparsed_row_amount

                self.parsed_rows.extend(zip(*self._parse_fetched_cols()))

    def execute(self,
                statement: str,
                params: list[tuple[Any]] | tuple[[tuple[Any]]] | None = None,
                data_as: str = 'rows',
                amount: int | None = None
                ):
        """Execute a statement. If params was provided - compile statement first
        and replace all question marks on passed parameters.

        :param statement: str a statement to execute with placeholders
        :param params: list[tuple[Any]] | tuple[tuple[Any]]: sequence of parameters to ingest into query
        :param data_as: string type of passed params. Possible values (`alchemy_flat_list`, `numpy`, `rows` - default)
        :param amount: int - amount of values of given params to insert

        Examples:
            query: SELECT * FROM <table_name> WHERE id IN (?, ?, ?) AND price >= ?;
            params: [(1, 2, 3, 450.69),]

            query: SELECT * FROM <table_name> WHERE id = %s AND price < %s;
            params: [(1, 200.00),]

            query: INSERT INTO <table_name> (id, name, description, price) VALUES (?, ?, ?, ?);
            params: [(1, 'Ryan Gosling', 'Actor', 0.0),]
            for next params it is better to use `executemany`
            params: [(1, 'Ryan Gosling', 'Actor', 0.0), (2, 'Mark Wahlberg', 'No pain no gain', 150.0)]

            query: UPDATE <table_name> SET price = ?, description = ? WHERE name = ?;
            params: [(999.999, 'Best actor', 'Ryan Gosling'),]

            query: DELETE FROM <table_name> WHERE id = ? AND other like ?;
            params: [(404, 200),]
        """

        if self.base_connection_closed:
            self.conn._verify_con_open()
        else:
            self.conn._verify_cur_open()

        self._execute_sqream_statement(statement, params=params, data_as=data_as, amount=amount)

        self._fill_description()
        self.rows_fetched = 0
        self.rows_returned = 0
        return self

    def executemany2(self, query, rows_or_cols=None, data_as='rows', amount=None):
        """Execute a statement, including parameterized data insert

        """

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

        self._send_columns()

        return self

    def executemany(self,
                    statement: str,
                    rows_or_cols: list[tuple[Any]] | tuple[[tuple[Any]]] | None = None,
                    data_as : str = 'rows',
                    amount: int | None = None
                    ) -> Cursor:
        """Execute a statement, preferably used for insertion

        :param statement: str a statement to execute with placeholders
        :param rows_or_cols: list[tuple[Any]] | tuple[tuple[Any]]: sequence of parameters to ingest into query
        :param data_as: string type of passed params. Possible values (`alchemy_flat_list`, `numpy`, `rows` - default)
        :param amount: int - amount of values of given params to insert
        """

        return self.execute(statement, params=rows_or_cols, data_as=data_as, amount=amount)

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

    def close_stmt(self) -> None:
        """Closes open statement with SQREAM

        Raises:
            ProgrammingError: If server responds with invalid JSON
            ProgrammingError: If server responds with valid JSON,
              but it is not object
            OperationalError: If server responds with "error" key in JSON
        """
        if self.open_statement:
            raw = self.client.send_string(
                '{"closeStatement": "closeStatement"}')

            # Check errors in response of the server
            if raw:
                try:
                    response = json.loads(raw)
                except json.decoder.JSONDecodeError:
                    log_and_raise(
                        ProgrammingError,
                        f"Could not parse server response: {raw}"
                    )
                if not isinstance(response, dict):
                    log_and_raise(
                        ProgrammingError,
                        f"Unexpected server response: {raw}"
                    )
                if "error" in response:
                    log_and_raise(OperationalError, response["error"])

            self.open_statement = False

            if logger.isEnabledFor(logging.INFO):
                logger.info(f'Done executing statement {self.stmt_id} over connection {self.conn.connection_id}')

    def _extract_nvarchar(self, idx, raw_col_data):
        if self.col_nul[idx]:
            col = [None if (_is_null(n)) else raw_col_data['data_column'][start:end].decode('utf8') for (start, end), n
                   in
                   zip(lengths_to_pairs(raw_col_data['true_nvarchar']), raw_col_data['nullable'])]
        else:
            col = [
                raw_col_data['data_column'][start:end].decode('utf8')
                for (start, end) in lengths_to_pairs(raw_col_data['true_nvarchar'])
            ]
        return col

    def _extract_varchar(self, idx, raw_col_data):
        varchar_size = self.col_type_tups[idx][1]
        if self.col_nul[idx]:
            col = []
            offset = 0
            for idx in raw_col_data['nullable']:
                if _is_null(idx):
                    col.append(None)
                    offset = offset + varchar_size
                else:
                    col.append(raw_col_data['data_column'][offset:offset + varchar_size].decode(self.conn.varchar_enc,
                                                                                                "ignore").replace(
                        '\x00', '').rstrip())
                    offset = offset + varchar_size
        else:
            col = [
                raw_col_data['data_column'][idx:idx + varchar_size].decode(
                    self.conn.varchar_enc, "ignore").replace('\x00', '').rstrip()
                for idx in range(0, len(raw_col_data['data_column']), varchar_size)
            ]
        return col

    def _extract_date(self, idx, raw_col_data):
        if self.col_nul[idx]:
            col = [sq_date_to_py_date(d, is_null=_is_null(n)) for d, n in
                   zip(raw_col_data['data_column'], raw_col_data['nullable'])]
        else:
            col = [sq_date_to_py_date(d) for d in raw_col_data['data_column']]
        return col

    def _extract_datetime(self, idx, raw_col_data):
        if self.col_nul[idx]:
            col = [sq_datetime_to_py_datetime(d, is_null=_is_null(n)) for d, n in
                   zip(raw_col_data['data_column'], raw_col_data['nullable'])]
        else:
            col = [sq_datetime_to_py_datetime(d) for d in raw_col_data['data_column']]
        return col

    def _extract_numeric(self, idx, raw_col_data):
        scale = self.col_type_tups[idx][2]
        if self.col_nul[idx]:
            col = [
                sq_numeric_to_decimal(raw_col_data['data_column'][idx:idx + 16], scale, is_null=_is_null(n))
                for idx, n in zip(range(0, len(raw_col_data['data_column']), 16), raw_col_data['nullable'])
            ]
        else:
            col = [
                sq_numeric_to_decimal(raw_col_data['data_column'][idx:idx + 16], scale)
                for idx in range(0, len(raw_col_data['data_column']), 16)
            ]
        return col

    def _extract_datatype(self, idx, raw_col_data):
        if self.col_nul[idx]:
            col = [None if _is_null(n) else d for d, n in zip(raw_col_data['data_column'], raw_col_data['nullable'])]
        else:
            col = raw_col_data['data_column']
        return col

    def _extract_array(
            self, idx: int, raw_col_data: memoryview) -> List[List[Any]]:
        """Extract array data from buffer

        Args:
            idx: integer index of extracting column in response
            raw_col_data: memoryview (bytes represenation) of data of
              column

        Returns:
            A list with Arrays of data. Array represented as python
            lists also.

            [[1, 5, 7], None, [31, 2, None, 6]]
        """
        sub_type_tup = self.col_type_tups[idx]
        typecode = typecodes.get(sub_type_tup[1])

        if typecode == "STRING":
            return self._extract_unfixed_array(raw_col_data)

        if typecode not in ("NUMBER", "DATETIME"):
            log_and_raise(
                NotSupportedError,
                f'Array of "{sub_type_tup[1]}" is not supported',
            )

        return self._extract_fixed_array(idx, raw_col_data)

    def _extract_fixed_array(
            self, idx: int, raw_col_data: memoryview) -> List[List[Any]]:
        """Extract array with data of fixed size

        Extract array from binary data of an Array with types of fixed
        size (BOOL, TINYINT, SMALLINT, INT, BIGINT, REAL, DOUBLE,
        NUMERIC, DATE, DATETIME). But not with TEXT

        Raw data contains binary data of nulls at each index in array
        and data separated by optional padding (trailing zeros at the
        end for portions of data whose lengths are not dividable by 8)

        Example for binary data for 1 row of boolean array[true, null,
        false]:
        `010 00000 100` -> replace paddings with _ `010_____100` where
        `010` are flag of null data inside array. Then `00000` is a
        padding to make lengths of data about nulls to be dividable by 8
        in case of array of length 8, 16, 24, 32 ... there won't be a
        padding then `100` is a binary representation of 3 boolean
        values itself

        Args:
            idx: integer index of extracting column in response
            raw_col_data: memoryview (bytes represenation) of data of
              column

        Returns:
            A list with Arrays of data. Array represented as python
            lists also.

            [[1, 5, 7], None, [31, 2, None, 6]]
        """
        buffer = raw_col_data['data_column']
        nulls_buffer = raw_col_data['nullable'] or false_generator()

        # Calculate size based on data_format
        data_size = struct.calcsize(type_to_letter[self.col_type_tups[idx][1]])
        trasform = self._get_trasform_func(idx)

        def _get_array(data: memoryview, nulls: memoryview, arr_size: int):
            """Construct one single array from data of type with fixed size"""
            # Do not use direct data.cast(data_format) to skip nulls
            # and process all data (including Numeric) by the same pattern
            return [
                trasform(data[i * data_size:(i + 1) * data_size], nulls[i])
                for i in range(arr_size)
            ]

        col = []
        start = 0
        for buf_len, null in zip(raw_col_data['array_lengths'], nulls_buffer):
            if null:
                col.append(None)
            else:
                array_size = get_array_size(data_size, buf_len)
                padding = (8 - array_size % 8) % 8

                # Slices of memoryview do not copy underlying data
                data = buffer[start + array_size + padding:start + buf_len]
                nulls = buffer[start:start + array_size]

                col.append(_get_array(data, nulls, array_size))
            start += buf_len
        return col

    def _extract_unfixed_array(
            self, raw_col_data: memoryview) -> List[List[Union[str, None]]]:
        """Extract array with data of unfixed size

        Extract array from binary data of an Array with types of TEXT
        - unfixed size

        Contains 8 bytes (long) that contains length of whole array
        (including nulls), binary data of nulls at each index in array
        and data separated by optional padding. Data here represents
        chunked info of each element inside array

        At the beginning, the data contains **cumulative** lengths
        (however is better to say indexes of their ends at data buffer)
        of all data strings (+ their paddings) of array as integers.
        The number of those int lengths is equal to the array length
        (those was in 8 bytes above) and because int take 4 bytes it all
        takes N * 4 bytes. Then if it is not divisible by 8 -> + padding
        Then the strings data also separated by optional padding

        Example for binary data for 1 row of text array['ABC','ABCDEF',null]:
        (padding zeros replaced with _)
        Whole buffer data: `3000000 001_____ 3000 14000 16000 ____ `
                           `65 66 67 _____ 65 66 67 68 69 70 __`
        Length of array: `3000000` -> long 3
        Nulls: `001_____`
        Length of strings: `3000 14000 16000 ____` -> 3,14,16 + padding
        Strings: `65, 66, 67, _____ 65, 66, 67, 68, 69, 70, __`
        L1 = 3, so [0 - 3) is string `65 66 67` -> ABC, padding P1=5
        L2 = 14 (which is L1 + padding + current_length), so
        current_length = L2 - (L1 + P1) = 14 - (5 + 3) = 6, P2=2
        => [5 + 3, 14) is string `65, 66, 67, 68, 69, 70` -> ABCDEF
        L3 = 16 => current_length = L3 - (L2 + P2) = 16 - (14 + 2) = 0
        thus string is empty, and considering Nulls -> it is a null

        Args:
            idx: integer index of extracting column in response
            raw_col_data: memoryview (bytes represenation) of data of
              column

        Returns:
            A list with Arrays of data. Array represented as python
            lists also.

            [["ABC", "ABCDEF", None], None, ["A", None, ""]]
        """
        # Code duplication with _extract_fixed_array could be eliminated
        # using template method with using separate Extractor classes
        buffer = raw_col_data['data_column']
        nulls_buffer = raw_col_data['nullable'] or false_generator()

        def _get_array(data: memoryview, nulls: memoryview, dlen: memoryview):
            """Construct one single array from data with dlen right bounds"""
            arr = []
            # lengths_to_pairs is not appropriate due to differences
            # in lengths representation
            for is_null, (strt, end) in zip(nulls, arr_lengths_to_pairs(dlen)):
                if is_null:
                    arr.append(None)
                else:
                    arr.append(data[strt:end].tobytes().decode('utf8'))
            return arr

        col = []
        start = 0
        for buf_len, null in zip(raw_col_data['array_lengths'], nulls_buffer):
            if null:
                col.append(None)
            elif not buf_len:
                col.append([])
            else:
                array_size = buffer[start: start + 8].cast('q')[0]  # Long
                padding = (8 - array_size % 8) % 8
                cur = start + 8 + array_size + padding
                # data lengths
                d_len = buffer[cur:cur + array_size * 4].cast('i')
                cur += (array_size + array_size % 2) * 4

                # Slices of memoryview do not copy underlying data
                data = buffer[cur:start + buf_len]
                nulls = buffer[start + 8:start + 8 + array_size]

                col.append(_get_array(data, nulls, d_len))

            start += buf_len
        return col

    def _get_trasform_func(self, idx: int) -> callable:
        """Provide function for casting bytes data to real data

        Args:
            idx: integer index of extracting column in response

        Returns:
            A function that cast simple portion of data to appropriate
            value.
        """
        type_tup = self.col_type_tups[idx]

        # Array's type_tup differs from others by adding extra string
        # at the beginning
        offset = 1 if type_tup[0] == 'ftArray' else 0
        data_format = type_to_letter[type_tup[offset]]
        wrappers = {
            'ftDate': sq_date_to_py_date,
            'ftDateTime': sq_datetime_to_py_datetime
        }

        if type_tup[offset] == 'ftNumeric':
            scale = type_tup[offset + 2]

            def cast(data):
                return sq_numeric_to_decimal(data, scale)
        elif type_tup[offset] in wrappers:
            def cast(data):
                return wrappers[type_tup[offset]](data.cast(data_format)[0])
        else:
            def cast(data):
                return data.cast(data_format)[0]

        def trasform(mem: memoryview, is_null: bool = False):
            return None if is_null else cast(mem)

        return trasform

    def close(self):
        self.close_stmt()
        self.conn.cur_closed = True
        self.conn.close_connection()
        self.closed = True
        self.buffer.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.close()

    def __iter__(self):
        for item in self.fetchall():
            yield item
