# cython: language_level=3, boundscheck=False
cimport cython
from cython cimport nogil, cfunc, boundscheck, wraparound 
from cython cimport cdivision, cast
from cython.parallel import prange 

from cpython.datetime cimport date, datetime
from cpython.array cimport array, clone
from cython.view cimport array as array_view
# from array import array
 

@nogil
@cfunc
def date_tuple_to_int_cy(year: cython.int, month: cython.int, day: cython.int) -> cython.int:

    mth: cython.int = (month + 9) % 12
    yr : cython.int = year - mth // 10
    
    return 365 * yr + yr // 4 - yr // 100 + yr // 400 + (mth * 306 + 5) // 10 + (day - 1)


@nogil
@cfunc
def datetime_tuple_to_long_cy(year: cython.int, month: cython.int, day: cython.int, hour: cython.int, minute: cython.int, second: cython.int, msecond: cython.int = 0) -> cython.long:
    ''' self contained to avoid function calling overhead '''

    mth     : cython.int = (month + 9) % 12
    yr      : cython.int = year - mth // 10
    date_int: cython.int = 365 * yr + yr // 4 - yr // 100 + yr // 400 + (mth * 306 + 5) // 10 + (day - 1)
    time_int: cython.int = hour * 3600 * 1000 + minute * 60 * 1000 + second * 1000 + msecond // 1000

    return (date_int << 32) + time_int


@cfunc
@boundscheck(False)
@wraparound(False)
def date_to_int_cy(date d) -> cython.int:

    year  : cython.int = d.year
    month : cython.int = d.month
    day   : cython.int = d.day

    mth   : cython.int = (month + 9) % 12
    yr    : cython.int = year - mth // 10
    
    return 365 * yr + yr // 4 - yr // 100 + yr // 400 + (mth * 306 + 5) // 10 + (day - 1)


@cfunc
@boundscheck(False)
@wraparound(False)
def datetime_to_long_cy(datetime dt) -> cython.long:

    year    : cython.int = dt.year
    month   : cython.int = dt.month
    day     : cython.int = dt.day
    hour    : cython.int = dt.hour
    minute  : cython.int = dt.minute
    second  : cython.int = dt.second
    msecond : cython.int = dt.microsecond

    mth     : cython.int = (month + 9) % 12
    yr      : cython.int = year - mth // 10
    date_int: cython.long = 365 * yr + yr // 4 - yr // 100 + yr // 400 + (mth * 306 + 5) // 10 + (day - 1)
    time_int: cython.long = hour * 3600 * 1000 + minute * 60 * 1000 + second * 1000 + msecond // 1000

    return (date_int << 32) + time_int


def date_to_int(date d) -> cython.int:

   return date_to_int_cy(d)


def datetime_to_long(datetime dt) -> cython.long:

    return datetime_to_long_cy(dt)

 # cython: language_level=3, boundscheck=False
cimport cython
from cython cimport nogil, cfunc, boundscheck, wraparound, cdivision, cast
from cython.parallel import prange 

from cython.view cimport array as array_view
from cpython.datetime cimport date, datetime
from cpython.array cimport array, clone
# from array import array
 

@nogil
@cfunc
def date_tuple_to_int_cy(year: cython.int, month: cython.int, day: cython.int) -> cython.int:

    mth: cython.int = (month + 9) % 12
    yr: cython.int = year - mth // 10
    
    return 365 * yr + yr // 4 - yr // 100 + yr // 400 + (mth * 306 + 5) // 10 + (day - 1)


@nogil
@cfunc
def datetime_tuple_to_long_cy(year: cython.int, month: cython.int, day: cython.int, hour: cython.int, minute: cython.int, second: cython.int, msecond: cython.int = 0) -> cython.long:
    ''' self contained to avoid function calling overhead '''

    mth: cython.int = (month + 9) % 12
    yr: cython.int = year - mth // 10
    date_int: cython.int = 365 * yr + yr // 4 - yr // 100 + yr // 400 + (mth * 306 + 5) // 10 + (day - 1)
    time_int: cython.int = hour * 3600 * 1000 + minute * 60 * 1000 + second * 1000 + msecond // 1000

    return (date_int << 32) + time_int



@cfunc
@boundscheck(False)
@wraparound(False)
def date_to_int_cy(date d) -> cython.int:

    year:  cython.int = d.year
    month: cython.int = d.month
    day:   cython.int = d.day

    mth: cython.int = (month + 9) % 12
    yr: cython.int = year - mth // 10
    
    return 365 * yr + yr // 4 - yr // 100 + yr // 400 + (mth * 306 + 5) // 10 + (day - 1)


@cfunc
@boundscheck(False)
@wraparound(False)
def datetime_to_long_cy(datetime dt) -> cython.long:

    year:    cython.int = dt.year
    month:   cython.int = dt.month
    day:     cython.int = dt.day
    hour:    cython.int = dt.hour
    minute:  cython.int = dt.minute
    second:  cython.int = dt.second
    msecond: cython.int = dt.microsecond

    mth: cython.int = (month + 9) % 12
    yr: cython.int = year - mth // 10
    date_int: cython.long = 365 * yr + yr // 4 - yr // 100 + yr // 400 + (mth * 306 + 5) // 10 + (day - 1)
    time_int: cython.long = hour * 3600 * 1000 + minute * 60 * 1000 + second * 1000 + msecond // 1000

    return (date_int << 32) + time_int


def date_to_int(date d) -> cython.int:

   return date_to_int_cy(d)


def datetime_to_long(datetime dt) -> cython.long:

    return datetime_to_long_cy(dt)


def dates_to_ints(date_list) -> cython.int:

    return [date_to_int_cy(d) for d in date_list]


def datetimes_to_longs(datetime_list) -> cython.long:

    return [datetimes_to_longs(dt) for dt in datetime_list]


## Numeric column packers
#  ----------------------

@cython.boundscheck(False)
@cython.wraparound(False)
def pack_dates(date_col, col_len, target = None, idx = None):
    list_bytes: array('b') = bytearray(4*col_len)
    cdef int[::1] view = memoryview(list_bytes).cast('i')
    
    cdef int i
    for i in range(col_len):
        # view[i] = date_to_int_cy(date_col[i])
        view[i] = next(date_col)
    
    # '''
    target.seek(idx)
    target.write(list_bytes)
    # '''
    
    return view


@cython.boundscheck(False)
@cython.wraparound(False)
def pack_datetimes(dt_col, col_len, target = None, idx = None):
    list_bytes: array('b') = bytearray(8*col_len)
    cdef long[::1] view = memoryview(list_bytes).cast('q')
    
    cdef int i
    for i in range(col_len):
        # view[i] = datetime_to_long_cy(dt_col[i])
        view[i] = next(dt_col)

    
    # '''
    target.seek(idx)
    target.write(list_bytes)
    # '''
    
    return view


@cython.boundscheck(False)
@cython.wraparound(False)
def pack_column(column, col_type):

    col_len: int = len(column) 

    return type_packer[col_type](column, col_len)

    

@cython.boundscheck(False)
@cython.wraparound(False)
def pack_bools(bool_col, col_len, target = None, idx = None):
    
    # cdef char[::1] view = memoryview(target[idx:]).cast('?')
    # '''
    list_bytes: array('b') = bytearray(len(bool_col))
    cdef char[::1] view = memoryview(list_bytes).cast('?')
    # '''

    cdef int i
    for i in range(len(bool_col)):
        view[i] = bool_col[i]
    
    # '''
    target.seek(idx)
    target.write(list_bytes)
    # '''
    
    return view


@cython.boundscheck(False)
@cython.wraparound(False)
def pack_tinyints(tinyint_col, col_len, target = None, idx = None):
    # cdef char[::1] view = memoryview(target[idx:]).cast('B')
    # '''
    list_bytes: array('b') = bytearray(len(tinyint_col))
    cdef char[::1] view = memoryview(list_bytes).cast('B')
    # '''

    cdef int i
    for i in range(len(tinyint_col)):
        view[i] = tinyint_col[i]
    
    # '''
    target.seek(idx)
    target.write(list_bytes)
    # '''
    
    return view



@cython.boundscheck(False)
@cython.wraparound(False)
def pack_shorts(short_col, col_len, target = None, idx = None):
    # cdef short[::1] view = memoryview(target[idx:]).cast('h')
    # '''
    list_bytes: array('b') = bytearray(2*len(short_col))
    cdef short[::1] view = memoryview(list_bytes).cast('h')
    # '''

    cdef int i
    for i in range(len(short_col)):
        view[i] = short_col[i]
    
    # '''
    target.seek(idx)
    target.write(list_bytes)
    # '''
    
    return view



@cython.boundscheck(False)
@cython.wraparound(False)
def pack_ints(int_col, col_len, target = None, idx = None):
    # cdef int[::1] view = memoryview(target[idx:]).cast('i')
    # '''
    list_bytes: array('b') = bytearray(4*len(int_col))
    cdef int[::1] view = memoryview(list_bytes).cast('i')
    # '''

    cdef int i
    for i in range(len(int_col)):
        view[i] = int_col[i]
    
    # '''
    target.seek(idx)
    target.write(list_bytes)
    # '''
    
    return view



@cython.boundscheck(False)
@cython.wraparound(False)
def pack_bigints(bigint_col, col_len, target = None, idx = None):
    # cdef long[::1] view = memoryview(target[idx:]).cast('q')
    # '''
    list_bytes: array('b') = bytearray(8*len(bigint_col))
    cdef long[::1] view = memoryview(list_bytes).cast('q')
    # '''

    cdef int i
    for i in range(len(bigint_col)):
        view[i] = bigint_col[i]
    
    # '''
    target.seek(idx)
    target.write(list_bytes)
    # '''
    
    return view



@cython.boundscheck(False)
@cython.wraparound(False)
def pack_floats(float_col, col_len, target = None, idx = None):
    # cdef float[::1] view = memoryview(target[idx:]).cast('f')
    # '''
    list_bytes: array('b') = bytearray(4*len(float_col))
    cdef float[::1] view = memoryview(list_bytes).cast('f')
    # '''

    cdef int i
    for i in range(len(float_col)):
        view[i] = float_col[i]
    
    # '''
    target.seek(idx)
    target.write(list_bytes)
    # '''
    
    return view



@cython.boundscheck(False)
@cython.wraparound(False)
def pack_doubles(double_col, col_len, target = None, idx = None):
    # cdef double[::1] view = memoryview(target[idx:]).cast('d')
    # '''
    list_bytes: array('b') = bytearray(8*len(double_col))
    cdef double[::1] view = memoryview(list_bytes).cast('d')
    # '''
    
    cdef int i
    for i in range(len(double_col)):
        view[i] = double_col[i]
    
    # '''
    target.seek(idx)
    target.write(list_bytes)
    # '''
    
    return view


@cython.boundscheck(False)
@cython.wraparound(False)
def pack_strings(string_list, col_len, target = None, idx = None):
    clump = ''.join(string_list)
    cl = len(clump)
    list_bytes: array('b') = bytearray(4*cl)
    cdef int[::1] view = memoryview(list_bytes)
    cdef int i
        
    for i in range(cl):
        view[i] = clump[i]
    
    # '''
    target.seek(idx)
    target.write(list_bytes)
    # '''
    
    return view


type_packer = {
    'ftBool': pack_bools,
    'ftUByte': pack_tinyints,
    'ftShort': pack_shorts,
    'ftInt': pack_ints,
    'ftLong': pack_bigints,
    'ftFloat': pack_floats,
    'ftDouble': pack_doubles,
    'ftDate': pack_dates,
    'ftDateTime': pack_datetimes
}
