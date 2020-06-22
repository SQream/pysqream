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
