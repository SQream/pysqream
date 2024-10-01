"""
Support functions for converting py values to sqream compatible values
and vice versa
"""
from __future__ import annotations

from datetime import datetime, date
from decimal import Decimal, getcontext
from math import floor, ceil, pow

import numpy as np


def pad_dates(num):
    return ('0' if num < 10 else '') + str(num)


def sq_date_to_py_date(sqream_date, is_null=False, date_convert_func=date):

    if is_null:
        return None

    year = (10000 * sqream_date + 14780) // 3652425
    intermed_1 = 365 * year + year // 4 - year // 100 + year // 400
    intermed_2 = sqream_date - intermed_1
    if intermed_2 < 0:
        year = year - 1
        intermed_2 = sqream_date - (365 * year + year // 4 - year // 100 +
                                    year // 400)
    intermed_3 = (100 * intermed_2 + 52) // 3060

    year = year + (intermed_3 + 2) // 12
    month = int((intermed_3 + 2) % 12) + 1
    day = int(intermed_2 - (intermed_3 * 306 + 5) // 10 + 1)

    return date_convert_func(year, month, day)


def sq_datetime_to_py_datetime(sqream_datetime, is_null=False, dt_convert_func=datetime):
    ''' Getting the datetime items involves breaking the long into the date int and time it holds
        The date is extracted in the above, while the time is extracted here  '''

    if is_null:
        return None

    date_part = sqream_datetime >> 32
    time_part = sqream_datetime & 0xffffffff
    date_part = sq_date_to_py_date(date_part, is_null=is_null)

    if date_part is None:
        return None

    msec = time_part % 1000
    sec = (time_part // 1000) % 60
    mins = (time_part // 1000 // 60) % 60
    hour = time_part // 1000 // 60 // 60
    return dt_convert_func(date_part.year, date_part.month, date_part.day,
                           hour, mins, sec, msec * int(pow(10, 3)))  # Python expects to get 6 digit on
                                                                     # miliseconds while SQream returns 3.


def _get_date_int(year: int, month: int, day: int) -> int:
    """Convert year, month and day to integer compatible with SQREAM"""
    month: int = (month + 9) % 12
    year: int = year - month // 10
    return (
        365 * year + year // 4 - year // 100 + year // 400
        + (month * 306 + 5) // 10 + (day - 1)
    )


def date_to_int(dat: date) -> int:
    """Convert datetime.date to integer compatible with SQREAM interface"""
    # datetime is also supported because it is descendant of date
    # date_to_int(date(1900, 1, 1)) is 693901 which is the oldest date that
    # sqream supports, so for None use the same
    return 693901 if dat is None else _get_date_int(*dat.timetuple()[:3])


def datetime_to_long(dat: datetime) -> int:
    """Convert datetime.datetime to integer (LONG) compatible with SQREAM"""
    if dat is None:
        # datetime_to_long(datetime(1900, 1, 1)) is 2980282101661696 which is
        # the oldest date that sqream supports, so for None use the same
        return 2980282101661696
    year, month, day, hour, minute, second = dat.timetuple()[:6]
    msec = dat.microsecond

    date_int: int = _get_date_int(year, month, day)
    time_int: int = 1000 * (hour * 3600 + minute * 60 + second) + msec // 1000

    return (date_int << 32) + time_int


tenth = Decimal("0.1")
if getcontext().prec < 38:
    getcontext().prec = 38


def sq_numeric_to_decimal(bigint_as_bytes: bytes, scale: int, is_null=False) -> [Decimal, None]:
    if is_null:
        return None

    getcontext().prec = 38
    c = memoryview(bigint_as_bytes).cast('i')
    bigint = ((c[3] << 96) + ((c[2] & 0xffffffff) << 64) + ((c[1] & 0xffffffff) << 32) + (c[0] & 0xffffffff))

    return Decimal(bigint) * (tenth ** scale)


def decimal_to_sq_numeric(dec: Decimal, scale: int) -> int:  # returns bigint
    if getcontext().prec < 38:
        getcontext().prec = 38
    res = dec * (10 ** scale)
    return ceil(res) if res > 0 else floor(res)


def lengths_to_pairs(nvarc_lengths):
    ''' Accumulative sum generator, used for parsing nvarchar columns '''

    idx = new_idx = 0
    for length in nvarc_lengths:
        new_idx += length
        yield idx, new_idx
        idx = new_idx


def arr_lengths_to_pairs(text_lengths):
    """Generator for parsing ARRAY TEXT columns' data"""
    start = 0
    for length in text_lengths:
        yield start, length
        start = length + (8 - length % 8) % 8


def numpy_datetime_str_to_tup(numpy_dt):
    ''' '1970-01-01T00:00:00.699148800' '''

    numpy_dt = repr(numpy_dt).split("'")[1]
    date_part, time_part = numpy_dt.split('T')
    year, month, day = date_part.split('-')
    hms, ns = time_part.split('.')
    hour, mins, sec = hms.split(':')
    return year, month, day, hour, mins, sec, ns


def numpy_datetime_str_to_tup2(numpy_dt):
    ''' '1970-01-01T00:00:00.699148800' '''

    ts = (numpy_dt - np.datetime64('1970-01-01T00:00:00Z')) / np.timedelta64(1, 's')
    dt = datetime.utcfromtimestamp(ts)

    return dt.year, dt.month, dt.day
