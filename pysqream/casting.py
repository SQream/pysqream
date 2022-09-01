from datetime import datetime, date, time as t
from decimal import Decimal, getcontext
from math import floor, ceil


def pad_dates(num):
    return ('0' if num < 10 else '') + str(num)


def sq_date_to_py_date(sqream_date, date_convert_func=date):
    if sqream_date is None or sqream_date == 0:
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


def sq_datetime_to_py_datetime(sqream_datetime, dt_convert_func=datetime):
    ''' Getting the datetime items involves breaking the long into the date int and time it holds
        The date is extracted in the above, while the time is extracted here  '''

    if sqream_datetime is None or sqream_datetime == 0:
        return None

    date_part = sqream_datetime >> 32
    time_part = sqream_datetime & 0xffffffff
    date_part = sq_date_to_py_date(date_part)

    if date_part is None:
        return None

    msec = time_part % 1000
    sec = (time_part // 1000) % 60
    mins = (time_part // 1000 // 60) % 60
    hour = time_part // 1000 // 60 // 60

    return dt_convert_func(date_part.year, date_part.month, date_part.day,
                           hour, mins, sec, msec)


def date_to_int(d: date) -> int:
    year, month, day = d.timetuple()[:3]
    mth: int = (month + 9) % 12
    yr: int = year - mth // 10

    return 365 * yr + yr // 4 - yr // 100 + yr // 400 + (mth * 306 + 5) // 10 + (day - 1)


def datetime_to_long(dt: datetime) -> int:
    ''' self contained to avoid function calling overhead '''

    year, month, day, hour, minute, second = dt.timetuple()[:6]
    msecond = dt.microsecond

    mth: int = (month + 9) % 12
    yr: int = year - mth // 10
    date_int: int = 365 * yr + yr // 4 - yr // 100 + yr // 400 + (
            mth * 306 + 5) // 10 + (day - 1)
    time_int: int = hour * 3600 * 1000 + minute * 60 * 1000 + second * 1000 + msecond // 1000

    return (date_int << 32) + time_int


tenth = Decimal("0.1")
if getcontext().prec < 38:
    getcontext().prec = 38


def sq_numeric_to_decimal(bigint_as_bytes: bytes, scale: int) -> Decimal:
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
