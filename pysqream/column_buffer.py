"""Buffer representation and writer's logic"""
import functools
import logging
import operator
import traceback
from decimal import Decimal
from mmap import mmap
from struct import pack_into, error as struct_error, pack
from typing import Any, List, Union, Tuple

import numpy as np

from pysqream.casting import date_to_int, datetime_to_long, decimal_to_sq_numeric
from pysqream.globals import WIN, ROWS_PER_FLUSH, VARCHAR_ENCODING, type_to_letter, ARROW, buf_maps, PYTYPES, typecodes
from pysqream.utils import DataError, ProgrammingError
from pysqream.logger import log_and_raise, logger, printdbg


class ColumnBuffer:
    """Buffer holding packed columns to be sent to SQream"""

    def __init__(self, *_, **__):
        ...

    def clear(self):
        if buf_maps:
            [buf_map.close() for buf_map in buf_maps[0]]

    def pack_columns(self, cols, capacity, col_types, col_sizes, col_nul, col_tvc, col_scales):
        """Packs the buffer starting a given index with the column. Returns number of bytes packed"""

        pool_params = list(zip(cols, range(len(col_types)), col_types,
                          col_sizes, col_nul, col_tvc, col_scales))

        if WIN:
            packed_cols = []
            for param_tup in pool_params:
                packed_cols.append(_pack_column(param_tup))

        else:
            try:
                packed_cols = [_pack_column(x) for x in pool_params]
            except DataError:
                raise  # Expected error, shouldn't be caught and wrapped
            except Exception as e:
                printdbg("Original error from pool.map: ", e)
                if logger.isEnabledFor(logging.ERROR):
                    logger.error("Original error from pool.map: ", e)
                log_and_raise(ProgrammingError,
                              "Error packing columns. Check that all types match the respective column types")

        return list(packed_cols)

    def close(self):
        self.clear()


## A top level packing function for Python's MP compatibility
def _pack_column(col_tup, return_actual_data=True):
    ''' Packs the buffer starting a given index with the column.
        Returns number of bytes packed '''

    global CYTHON
    col, col_idx, col_type, size, nullable, tvc, scale = col_tup
    if "ftArray" in col_type:
        return _pack_array(col, col_idx, col_type[1], nullable, scale)

    col = list(col)
    capacity = len(col)
    buf_idx = 0
    # Don't need to initialize bit mmap, could be created after gathering
    # dynamic array (list) directly into bytes
    # TODO: replace logic using list
    buf_map = mmap(-1, ((1 if nullable else 0) + (size if size != 0 else 104)) * ROWS_PER_FLUSH)
    buf_view = memoryview(buf_map)

    def pack_exception(e):
        ''' Allowing to return traceback info from parent process when using mp.Pool on _pack_column
            [add link]
        '''

        e.traceback = traceback.format_exc()
        error_msg = f'Trying to insert unsuitable types to column number {col_idx + 1} of type {col_type}'
        logger.error(error_msg, exc_info=True)
        raise ProgrammingError(error_msg)

    # Numpy array for column
    if ARROW and isinstance(col, np.ndarray):
        # Pack null column if applicable
        if nullable:
            pack_into(f'{capacity}b', buf_view, buf_idx,
                      *[1 if item in (np.nan, b'') else 0 for item in col])
            buf_idx += capacity

            # Replace Nones with appropriate placeholder
            if 'S' in repr(col.dtype):  # already b''?
                pass
            elif 'U' in repr(col.dtype):
                pass
            else:
                # Swap out the nans
                col[col == np.nan] = 0

        # Pack nvarchar length column if applicable
        if tvc:
            buf_map.seek(buf_idx)
            lengths_as_bytes = np.vectorize(len)(col).astype('int32').tobytes()
            buf_map.write(lengths_as_bytes)
            buf_idx += len(lengths_as_bytes)

        # Pack the actual data
        if 'U' in repr(col.dtype):
            packed_strings = ''.join(col).encode('utf8')
            buf_map.seek(buf_idx)
            buf_map.write(packed_strings)
            buf_idx += len(packed_strings)
            # print(f'unicode strings: {packed_strings}')
        else:
            packed_np = col.tobytes()
            buf_map.seek(buf_idx)
            buf_map.write(packed_np)
            buf_idx += len(packed_np)

        return buf_map[0:buf_idx] if return_actual_data else (0, buf_idx)

    # Pack null column if applicable
    type_code = type_to_letter[col_type]

    # Pack null column and replace None with appropriate placeholder
    col_placeholder = {
        'ftBool': 0,
        'ftUByte': 0,
        'ftShort': 0,
        'ftInt': 0,
        'ftLong': 0,
        'ftFloat': 0,
        'ftDouble': 0,
        'ftDate': None,  # updated separately
        'ftDateTime': None,  # updated separately
        'ftVarchar': ''.ljust(size, ' '),
        'ftBlob': '',
        'ftNumeric': 0
    }

    if nullable:
        idx = -1
        # TODO: Replace while True and index with just traversal without idx
        # for better time complexity
        while True:
            try:
                idx = col.index(None, idx + 1)
            except ValueError:
                break
            else:
                buf_map.seek(buf_idx + idx)
                buf_map.write(b'\x01')
                col[idx] = col_placeholder[col_type]
        # buf_map.seek(buf_idx)
        # buf_map.write(nulls)
        buf_idx += capacity

    # If a text column, replace and pack in adavnce to see if the buffer is sufficient
    if col_type == 'ftBlob':
        try:
            encoded_col = [strn.encode('utf8') for strn in col]
        except AttributeError as e:  # Non strings will not have .encode()
            pack_exception(e)
        else:
            packed_strings = b''.join(encoded_col)

        needed_buf_size = len(packed_strings) + 5 * capacity

        # Resize the buffer if not enough space for the current strings
        if needed_buf_size > len(buf_map):
            buf_view.release()
            buf_map.resize(needed_buf_size)
            buf_view = memoryview(buf_map)

        # Pack nvarchar length column
        pack_into(f'{capacity}i', buf_view, buf_idx, *[len(string) for string in encoded_col])

        buf_idx += 4 * capacity

    # Replace Nones with appropriate placeholder - this affects the data itself
    if col_type == 'ftBlob':
        pass  # Handled preemptively due to allow possible buffer resizing

    elif col_type == 'ftVarchar':
        try:
            col = (strn.encode(VARCHAR_ENCODING)[:size].ljust(size, b' ') for strn in col)
        except AttributeError as e:  # Non strings will not have .encode()
            pack_exception(e)
        else:
            packed_strings = b''.join(col)

    elif col_type == 'ftDate':
        try:
            col = (date_to_int(deit) for deit in col)
        except AttributeError as e:  # Non date/times will not have .timetuple()
            pack_exception(e)

    elif col_type == 'ftDateTime':
        try:
            col = (datetime_to_long(dt) for dt in col)
        except AttributeError as e:
            pack_exception(e)

    elif col_type == 'ftNumeric':
        try:
            col = (decimal_to_sq_numeric(Decimal(num), scale) for num in col)
        except AttributeError as e:
            pack_exception(e)

    elif col_type in ('ftBool', 'ftUByte', 'ftShort', 'ftInt', 'ftLong', 'ftFloat', 'ftDouble'):
        pass
    else:
        error_msg = f'Bad column type passed: {col_type}'
        logger.error(error_msg, exc_info=True)
        raise ProgrammingError(error_msg)

    CYTHON = False
    # Done preceding column handling, pack the actual data
    if col_type in ('ftVarchar', 'ftBlob'):
        buf_map.seek(buf_idx)
        buf_map.write(packed_strings)
        buf_idx += len(packed_strings)
    elif col_type == 'ftNumeric':
        buf_map.seek(buf_idx)
        all = functools.reduce(operator.iconcat, (num.to_bytes(16, byteorder='little', signed=True) for num in col), [])
        buf_map.write(bytearray(all))
        buf_idx += len(all)
    else:
        try:
            if CYTHON:
                buf_map.seek(buf_idx)
                # type_packer[col_type](col, size, buf_map, buf_idx)
            else:
                pack_into(f'{capacity}{type_code}', buf_view, buf_idx, *col)
        except struct_error as e:
            pack_exception(e)

        buf_idx += capacity * size

    return buf_map[0:buf_idx] if return_actual_data else (0, buf_idx)


def _pack_array(col: List[Union[List[Any], None]],
                col_idx: int,
                col_type: str,
                nullable: bool,
                scale: int
                ):
    """Pack array of any data type to bytes for transmitting to SQREAM

    Acceptable format of bytes includes 3 parts connected together
    without any padding gaps between:
        part1: nulls if nullable just N elements without padding
        part2: lengths of data chunks in part3 for each N row
            just N integers without padding (mem N * 4)
        part3: data without padding (varies depend on type of array)

    Explanation on part3 for fixed SIZE data types:
        First come 1 byte that explains whether data at first index is
        a null or not, then come SIZE bytes with the value, then again
        1 byte and SIZE bytes etc. When row ends, then just new array
        started without padding by the same pattern.

        Example of ARRAY INT[] of 3 rows:
        array[1, null, 8], null, array[2, 5]
        Packed: `010  15000 0000 10000  0 1000 1 0000 0 8000  0 2000 0 5000`
                 ^    ^                 ^                     ^
            p1 nulls  p2 lengths        p3 data row1          row3

    Explanation on part3 for unfixed size data type (TEXT):
        After part1 & 2 (the same)
        Started with array size in first 4 bytes (INT value)
        Then for each N elements: 1 byte (is null), 4 byte - INT represents
        LENGTH of string at this index, then LENGTH bytes data of string
        itself. Then the same for each row

        Example of ARRAY TEXT[] of 3 rows:
        array['WORK', '', null, "IS DONE"], null, array['Ok', 'then']
        Full buffer:
            part1 and 2: `010  35 000 0000 20 000`
                          ^    ^
                     p1 nulls  p2 lengths
            row1: `4000  0 4000 WORK  0 0000  1 0000  0 7000 IS DONE`
                   ^     ^            ^       ^       ^
            Array size   idx=1        idx=2   idx=3   idx=4
            row2: ``  - empty because it is null
            row3: `2000  0 2000 Ok  0 4000 then`
                   ^     ^          ^
            Array size   idx=1      idx=2

    Args:
        col: list of rows that could be lists that represents array or None
        col_idx: integer index of inserting column,
        col_type: string (enum) that represents data type that array contains
          could be ftBool, ftInt, etc
        nullable: boolean that show whether column is nullable or not,
        scale: integer represent Numerical Rating Scale, for numerical only

    Returns:
        A bytes of packed array that could be sent to SQREAM

        Examples from description:
        1. b'\x00\x01\x00\x0f\x00\x00\x00\x00\x00\x00\x00\n\x00\x00\x00'
           b'\x00\x01\x00\x00\x00\x01\x00\x00\x00\x00\x00\x08\x00\x00'
           b'\x00\x00\x02\x00\x00\x00\x00\x05\x00\x00\x00'
        2. b'\x00\x01\x00#\x00\x00\x00\x00\x00\x00\x00\x14\x00\x00\x00'
           b'\x04\x00\x00\x00\x00\x04\x00\x00\x00WORK\x00\x00\x00\x00'
           b'\x00\x01\x00\x00\x00\x00\x00\x07\x00\x00\x00IS DONE\x02'
           b'\x00\x00\x00\x00\x02\x00\x00\x00Ok\x00\x04\x00\x00\x00then'

    Raises:
      DataError: If None row is present for not nullable column
      DataError: If type value inside array (list) does not match
        appropriate type for column and it is not None
    """
    py_types = PYTYPES[col_type]

    trasform: callable = _get_backward_transform_func(col_type, scale)

    nulls = []
    lengths = []
    data_list = []
    for row in col:
        if nullable:
            nulls.append(pack('?', row is None))
        elif row is None:
            raise DataError(
                f"There is null in data for not nullable column {col_idx}")

        d_size = 0

        if row is not None and len(row) > 0:
            if typecodes[col_type] == 'STRING':
                data_list.append(pack('i', len(row)))
                d_size = 4
            for val in row:
                is_null = val is None
                if not is_null and not isinstance(val, py_types):
                    raise DataError(
                        f"Array contains inappropriate type: {type(val)}. "
                        f"Only None and {py_types} are allowed")
                data_list.append(pack('?', is_null))
                data_list.append(trasform(val))
                # Counting data size in runtime allows to use the same
                # code of both fixed and unfixed data types
                # len is always O(1)
                d_size += len(data_list[-1]) + 1
        lengths.append(pack('i', d_size))

    res = b''.join(nulls + lengths + data_list)
    return res


def _get_backward_transform_func(col_type: Union[Tuple[str], str], scale: int):
    """
    Provide function for casting py data to bytes that SQREAM expects

    Args:
        col_type: string (enum) that represent SQREAM data type
          OR tuple with strings that represent ftArray and its' data type
        scale: integer represent Numerical Rating Scale, for numerical only

    Returns:
        A callable that take one value - data and transform it into
          SQREAM compatible bytes
    """

    # Array's type_tup differs from others by adding extra string
    # at the beginning
    if 'ftArray' in col_type:
        return _get_backward_transform_func(col_type[1], scale)

    data_format = type_to_letter[col_type]
    wrappers = {
        'ftDate': date_to_int,
        'ftDateTime': datetime_to_long,
    }

    if col_type == 'ftNumeric':
        def cast(data: Any) -> bytes:
            val = decimal_to_sq_numeric(data, scale) if data else 0
            return val.to_bytes(16, byteorder='little', signed=True)
    elif col_type in wrappers:
        def cast(data: Any) -> bytes:
            return pack(data_format, wrappers[col_type](data))
    elif typecodes[col_type] == 'STRING':
        def cast(data: str) -> bytes:
            if not data:
                return b'\x00\x00\x00\x00'
            return pack(f'i{len(data)}s', len(data), data.encode())
    else:
        def cast(data: Any) -> bytes:
            return pack(data_format, data or 0)

    return cast
