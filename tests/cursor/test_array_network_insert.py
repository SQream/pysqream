"""
Test for insertion array via network

This imply using syntax:
    # Get a new cursor
    cur = con.cursor()
    insert = "insert into perf values (?,?)"  # insert in 2 columns
    cur.executemany(insert, [
        (array_col1_row1, array_col2_row1),  # row1
        (array_col1_row2, array_col2_row2),  # row2
    ])
    # Close this cursor
    cur.close()
"""
from datetime import date, datetime
from decimal import Decimal

import pytest
import numpy as np  # numpy is currently in requirements of the package

from pysqream.errors import DataError, OperationalError
from pysqream.globals import ROWS_PER_FLUSH

from ..utils import ALL_TYPES, SIMPLE_VALUES, ensure_empty_table, select

TEMP_TABLE = "test_array_network_insert_temp"


WRONG_TYPES_VALUES = [
    [True, False, "What?"],
    [1, 255, 255.1],
    [256, 32767, 3.5],
    [32768, 2147483647, 1.0123],
    [2147483648, 9223372036854775807, 1.5],

    [Decimal('0.12324567890123456789012345678901234567'), "Don't do that"],
    [Decimal('131235.1232'), [Decimal('131235.1232')]],

    [date(1955, 11, 5), '9999-12-31'],  # Does not accept strings
    [datetime(1955, 11, 5, 1, 24), date(1955, 11, 5)],

    ["Kiwis have tiny wings, but cannot fly.", 12331, b'No bytes!'],
]


@pytest.fixture(name='cursor')
def cursor_with_arrays_allowed(cursor):
    """Redefined cursor fixture that enables arrays for this tests module"""
    cursor.conn.allow_array = True
    yield cursor


@pytest.mark.slow
@pytest.mark.parametrize("data_type, data", [
    ("BOOL", [True, False, True, None, True]),
    ("TINYINT", [2, None, 255]),  # less than 256 (1 byte)
    ("SMALLINT", [None, 32767, 256, 7]),
    ("INT", [2147483647, 32768, None, 56]),
    ("BIGINT", [9223372036854775807, 2147483647, 1, None]),
    ("REAL", [3.1410000324249268, None, 5.315000057220459, 1.0]),
    ("DOUBLE", [0.000003, None, 10.000001, 101.000026, 2.3]),
    ("NUMERIC(38,38)", [
        Decimal('0.12324567890123456789012345678901234567'), None]),
    ("NUMERIC(12,4)", [Decimal('131235.1232'), None]),
    ("DATE", [date(1955, 11, 5), None, date(9999, 12, 31)]),
    ("DATETIME", [datetime(1955, 11, 5, 1, 24),
                  datetime(9999, 12, 31, 23, 59, 59, 999000), None]),
    ("TEXT", [None, "Kiwis have tiny wings, but cannot fly.", "", "xXx"]),
])
def test_insert_most_types(cursor, data_type, data):
    """Test insert most types except DATETIME"""
    ensure_empty_table(cursor, TEMP_TABLE, f"d {data_type}[]")

    cursor.executemany(f"insert into {TEMP_TABLE} values (?)", [(data,)])

    assert select(cursor, TEMP_TABLE) == [(data, )]


@pytest.mark.parametrize("year, month, day, hour, minute, sec, mcrsec", [
    (1955, 11, 5, 1, 24, 0, 111),
    (9999, 12, 31, 23, 59, 59, 888444),
    # (9999, 12, 31, 23, 59, 59, 888999),  # Won't pass while SQ-13969
])
def test_insert_datetime(cursor, year, month, day, hour, minute, sec, mcrsec):
    """
    Test insert array of datetime works such buggy as DATATIME itself

    Should move to test_insert_most_types after fixing bugs:
    Bug2 (INSERT) https://sqream.atlassian.net/browse/SQ-13969
    """
    # Passing many argument allows to see datetime in tests explicitly, so
    # pylint: disable=too-many-arguments
    ensure_empty_table(cursor, TEMP_TABLE, "d DATETIME[]")

    data = datetime(year, month, day, hour, minute, sec, mcrsec)
    data2 = datetime(year - 1, month, day, hour, minute, sec, mcrsec)
    cursor.executemany(
        f"insert into {TEMP_TABLE} values (?)", [([data, data2], )])

    # adjust to working in current bugs environment
    res1 = datetime(year, month, day, hour, minute, sec, round(mcrsec, -3))
    res2 = datetime(year - 1, month, day, hour, minute, sec, round(mcrsec, -3))
    assert select(cursor, TEMP_TABLE) == [([res1, res2], )]


@pytest.mark.parametrize("_type", ALL_TYPES)
def test_insert_all_types_send_none(cursor, _type):
    """Test that None send appropriate values for nullable columns"""
    ensure_empty_table(cursor, TEMP_TABLE, f"d {_type}[] null")

    data = [(None,)] * 15
    cursor.executemany(f"insert into {TEMP_TABLE} values (?)", data)
    assert select(cursor, TEMP_TABLE) == data


@pytest.mark.parametrize("_type", ALL_TYPES)
def test_insert_all_types_send_none_for_not_nullable(cursor, _type):
    """Test that DataError raises in case of sending Nones to not nullable"""
    ensure_empty_table(cursor, TEMP_TABLE, f"d {_type}[] not null")

    with pytest.raises(DataError):
        cursor.executemany(f"insert into {TEMP_TABLE} values (?)", [(None,)])


@pytest.mark.slow
@pytest.mark.parametrize("size", [2, 10, 50])
@pytest.mark.parametrize("_type", ALL_TYPES)
def test_insert_fixed_size_array_works(cursor, _type, size):
    """Test that insert works with lists of length less or equal to defined"""
    ensure_empty_table(cursor, TEMP_TABLE, f"d {_type}[{size}]")

    data = [([None] * i, ) for i in range(size + 1)] * 15
    cursor.executemany(f"insert into {TEMP_TABLE} values (?)", data)
    assert select(cursor, TEMP_TABLE) == data


@pytest.mark.parametrize("size", [2, 10, 50])
@pytest.mark.parametrize("_type", ALL_TYPES)
def test_insert_fixed_size_array_exceed_range_raises(cursor, _type, size):
    """Test that insert raises with lists of length greater than defined"""
    ensure_empty_table(cursor, TEMP_TABLE, f"d {_type}[{size - 1}]")
    data = [([None] * i, ) for i in range(size + 1)] * 15
    with pytest.raises(OperationalError, match=r"Array size \d+ exceeds \w+ "
                                               r"column limit \d+."):
        # Should not validate response
        cursor.executemany(f"insert into {TEMP_TABLE} values (?)", data)


@pytest.mark.parametrize("_type", ALL_TYPES)
def test_insert_all_types_send_empty_list(cursor, _type):
    """Test insertion of empty lists"""
    ensure_empty_table(cursor, TEMP_TABLE, f"d {_type}[]")
    data = [([],)] * 10
    cursor.executemany(f"insert into {TEMP_TABLE} values (?)", data)
    assert select(cursor, TEMP_TABLE) == data


@pytest.mark.parametrize("_type, val", zip(ALL_TYPES, SIMPLE_VALUES))
def test_insert_more_rows_than_per_flush(cursor, _type, val):
    """Test insertion works with sending rows by parts"""
    ensure_empty_table(cursor, TEMP_TABLE, f"d {_type}[]")
    data = [(val,)] * (ROWS_PER_FLUSH + 2)
    cursor.executemany(f"insert into {TEMP_TABLE} values (?)", data)
    res = select(cursor, TEMP_TABLE)
    # Do some lightweight checks before full compare to make it easier
    # both to debug and to provide comparison by pytest
    assert len(res) == len(data)
    assert res[0] == data[0]
    mid = int(ROWS_PER_FLUSH / 2)
    assert res[mid] == data[mid]
    assert res[-1] == data[-1]
    assert res == data


@pytest.mark.parametrize("_type, val", zip(ALL_TYPES, WRONG_TYPES_VALUES))
def test_inappropriate_type_raises(cursor, _type, val):
    """Test wrong python types raises DataError"""
    ensure_empty_table(cursor, TEMP_TABLE, f"d {_type}[]")
    data = [(val, )]
    with pytest.raises(DataError):
        cursor.executemany(f"insert into {TEMP_TABLE} values (?)", data)
    assert select(cursor, TEMP_TABLE) == []


@pytest.mark.parametrize("_type, val", zip(ALL_TYPES, SIMPLE_VALUES))
def test_numpy_arrays(cursor, _type, val):
    """Test that numpy data types works as well as python built-ins"""
    ensure_empty_table(cursor, TEMP_TABLE, f"d {_type}[]")
    data = [(np.array(val),)]
    cursor.executemany(f"insert into {TEMP_TABLE} values (?)", data)
    res = select(cursor, TEMP_TABLE)
    assert res == [(val,)]
