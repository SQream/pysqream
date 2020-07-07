# cython: language_level=3, boundscheck=False
cimport cython
from cython cimport nogil, cfunc, boundscheck, wraparound 
from cython cimport cdivision, cast
from cython.parallel import prange 

from cpython.datetime cimport date, datetime
# from cpython.array cimport array, clone
# from cython.view cimport array as array_view



# @cfunc
@boundscheck(False)
@wraparound(False)
def sq_date_to_py_date_cy(cython.long sqream_date) -> date:

    sqream_date : cython.uint = <unsigned int> sqream_date
    year        : cython.long = (10000 * sqream_date + 14780) // 3652425
    intermed_1  : cython.long = 365 * year + year//4 - year//100 + year//400
    intermed_2  : cython.long = sqream_date - intermed_1
    if intermed_2 < 0:
        year = year - 1
        intermed_2 = sqream_date - (365*year + year//4 - year//100 + year//400)
    
    intermed_3 : cython.uint= (100 * intermed_2 + 52) // 3060

    year = year + (intermed_3 + 2) // 12
    month : cython.uint = int((intermed_3 + 2) % 12) + 1
    day   : cython.uint = int(intermed_2 - (intermed_3 * 306 + 5) // 10 + 1)

    print ("ymd:", year, month, day)
    return date(year, month, day)


# @cfunc
@boundscheck(False)
@wraparound(False)
def sq_datetime_to_py_datetime_cy(cython.long sqream_datetime) -> datetime:


    sqream_date : cython.uint = sqream_datetime >> 32
    time_part : cython.uint = sqream_datetime & 0xffffffff
    date_part : date = sq_date_to_py_date_cy(sqream_date)

    msec : cython.uint = time_part % 1000
    sec  : cython.uint = (time_part // 1000) % 60
    mins : cython.uint = (time_part // 1000 // 60) % 60
    hour : cython.uint = time_part // 1000 // 60 // 60

    print ("dt:", date_part.year, date_part.month, date_part.day, hour, mins, sec, msec)
    return datetime(date_part.year, date_part.month, date_part.day, hour, mins, sec, msec)



def sq_date_to_py_date(sqream_date) -> date:

   return sq_date_to_py_date_cy(sqream_date)


def sq_datetime_to_py_datetime(sqream_datetime) -> datetime:

    return sq_datetime_to_py_datetime_cy(sqream_datetime)
