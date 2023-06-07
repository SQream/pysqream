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

from .utils import assert_table_empty

TEMP_TABLE = "test_array_network_insert_temp"


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
    # ("DATETIME", [datetime(1955, 11, 5, 1, 24),
    #               datetime(9999, 12, 31, 23, 59, 59, 999), None]),
    ("TEXT", [None, "Kiwis have tiny wings, but cannot fly.", "", "xXx"]),
])
def test_insert_most_types(cursor, data_type, data):
    """Test insert most types except DATETIME"""
    cursor.execute(f"CREATE OR REPLACE TABLE {TEMP_TABLE} (d {data_type}[])")
    assert_table_empty(cursor, TEMP_TABLE)

    cursor.executemany(f"insert into {TEMP_TABLE} values (?)", [(data,)])

    cursor.execute(f"SELECT * FROM {TEMP_TABLE};")
    result = cursor.fetchall()
    assert result == [(data, )]


@pytest.mark.parametrize("year, month, day, hour, minute, sec, mcrsec", [
    (1955, 11, 5, 1, 24, 0, 0),
    (9999, 12, 31, 23, 59, 59, 999),
])
def test_insert_datetime(cursor, year, month, day, hour, minute, sec, mcrsec):
    """
    Test insert array of datetime works such buggy as DATATIME itself

    Should move to test_insert_most_types after fixing bugs:
    Bug1 (SELECT) https://sqream.atlassian.net/browse/SQ-13967
    Bug2 (INSERT) https://sqream.atlassian.net/browse/SQ-13969
    """
    # Passing many argument allows to see datetime in tests explicitly, so
    # pylint: disable=too-many-arguments
    cursor.execute(f"CREATE OR REPLACE TABLE {TEMP_TABLE} (d DATETIME[])")
    assert_table_empty(cursor, TEMP_TABLE)

    data = datetime(year, month, day, hour, minute, sec, mcrsec)
    data2 = datetime(year - 1, month, day, hour, minute, sec, mcrsec)
    cursor.executemany(
        f"insert into {TEMP_TABLE} values (?)", [([data, data2], )])

    cursor.execute(f"SELECT * FROM {TEMP_TABLE};")
    result = cursor.fetchall()
    # adjust to working in current bugs environment
    res1 = datetime(year, month, day, hour, minute, sec, mcrsec // 1000)
    res2 = datetime(year - 1, month, day, hour, minute, sec, mcrsec // 1000)
    assert result == [([res1, res2], )]


def test_insert_all_types_send_none(cursor):
    ...

def test_insert_all_types_send_none_for_not_nullable(cursor):
    ...

def test_insert_fixed_size_array_exceed_value_range_raises(cursor):
    ...

def test_insert_all_types_send_empty_list(cursor):
    ...

def test_insert_more_rows_than_per_flush(cursor):
    ...

def test_inappropriate_type_raises(cursor):
    ...
