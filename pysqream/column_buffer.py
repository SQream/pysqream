from pysqream.casting import *
from pysqream.globals import WIN, BUFFER_SIZE, ROWS_PER_FLUSH, VARCHAR_ENCODING, type_to_letter, ARROW, buf_maps, buf_views
from pysqream.utils import NotSupportedError, ProgrammingError, InternalError, IntegrityError, OperationalError, DataError, \
    DatabaseError, InterfaceError, Warning, Error

from pysqream.logger import *
from decimal import Decimal
import traceback
import array
from struct import pack_into, error as struct_error
import multiprocessing as mp
import numpy as np
from mmap import mmap
from functools import reduce
import functools
import operator


def init_lock(l):

    """To pass a lock to mp.Pool()"""

    global lock
    lock = l


class ColumnBuffer:
    ''' Buffer holding packed columns to be sent to SQream '''

    def __init__(self, size=BUFFER_SIZE):
        global buf_maps, buf_views

    def clear(self):
        if buf_maps:
            [buf_map.close() for buf_map in buf_maps[0]]

    def init_buffers(self, col_sizes, col_nul):
        if not WIN:
            try:
                self.pool.close()
                self.pool.join()
            except Exception as e:
                pass

            l = mp.Lock()
            self.pool = mp.Pool(initializer=init_lock, initargs=(l,))
        self.clear()
        buf_maps = [mmap(-1, ((1 if col_nul else 0) + (size if size != 0 else 104)) * ROWS_PER_FLUSH) for size in
                    col_sizes]
        buf_views = [memoryview(buf_map) for buf_map in buf_maps]

    def pack_columns(self, cols, capacity, col_types, col_sizes, col_nul, col_tvc, col_scales):
        ''' Packs the buffer starting a given index with the column.
            Returns number of bytes packed '''

        pool_params = zip(cols, range(len(col_types)), col_types,
                          col_sizes, col_nul, col_tvc, col_scales)
        if WIN:
            packed_cols = []
            for param_tup in pool_params:
                packed_cols.append(_pack_column(param_tup))

        else:
            # self.pool = mp.Pool()
            # To use multiprocess type packing, we call a top level function with a single tuple parameter
            try:
                packed_cols = self.pool.map(_pack_column, pool_params, chunksize=2)  # buf_end_indices
            except Exception as e:
                printdbg("Original error from pool.map: ", e)
                if logger.isEnabledFor(logging.ERROR):
                    logger.error("Original error from pool.map: ", e)
                log_and_raise(ProgrammingError,
                              "Error packing columns. Check that all types match the respective column types")

        return list(packed_cols)

    def close(self):
        self.clear()
        try:
            self.pool.close()
            self.pool.join()
        except Exception as e:
            # print (f'testing pool closing, got: {e}')
            pass  # no pool was initiated


## A top level packing function for Python's MP compatibility
def _pack_column(col_tup, return_actual_data=True):
    ''' Packs the buffer starting a given index with the column.
        Returns number of bytes packed '''

    global CYTHON
    col, col_idx, col_type, size, nullable, tvc, scale = col_tup
    col = list(col)
    capacity = len(col)
    buf_idx = 0
    buf_map = mmap(-1, ((1 if nullable else 0) + (size if size != 0 else 104)) * ROWS_PER_FLUSH)
    buf_view = memoryview(buf_map)

    def pack_exception(e):
        ''' Allowing to return traceback info from parent process when using mp.Pool on _pack_column
            [add link]
        '''

        e.traceback = traceback.format_exc()
        error_msg = f'Trying to insert unsuitable types to column number {col_idx + 1} of type {col_type}'
        with lock:
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
        # date_tuple_to_int(1900, 1, 1) = 693901
        pass
        # '''
        try:
            col = (date_to_int(deit) if deit is not None else 693901 for deit in col)
        except AttributeError as e:  # Non date/times will not have .timetuple()
            pack_exception(e)
        # '''

    elif col_type == 'ftDateTime':
        # datetime_tuple_to_long(1900, 1, 1, 0, 0, 0) = 2980282101661696
        try:
            col = (datetime_to_long(dt) if dt is not None else 2980282101661696 for dt in col)
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
        with lock:
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