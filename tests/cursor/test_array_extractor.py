"""Test fetching SQREAM Arrays of all datatype"""
from datetime import date, datetime
from decimal import Decimal

import pytest


TEMP_TABLE = "test_array_fetch_temp"


@pytest.fixture(name='array_table')
def generate_array_table(cursor, request):
    """Fixture that generate table, insert data and drop table"""
    _type, values = request.param
    cursor.execute(
        f"CREATE OR REPLACE TABLE {TEMP_TABLE} (data array({_type}));")
    insert_values = ', '.join([f"(ARRAY({v}))" for v in values])
    cursor.execute(f"INSERT INTO {TEMP_TABLE} VALUES {insert_values}")
    yield
    cursor.execute(f"DROP TABLE {TEMP_TABLE};")


DATATYPES_DATA = [
    ["BOOL", ["false, true, null"]],
    ["TINYINT", ["2, null, 255"]],
    ["SMALLINT", ["null, 32767, 256, 7"]],
    ["INT", ["2147483647, 32768, null, 56"]],
    ["BIGINT", ["9223372036854775807, 2147483647, 1, null"]],

    ["REAL", ["3.141, null, 5.315, 1.0"]],
    ["DOUBLE", ["0.000003, null, 10.000001, 101.000026, 2.3"]],
    ["NUMERIC(38,38)", ["0.12324567890123456789012345678901234567, null"]],
    ["NUMERIC(12,4)", ["131235.123245678901234567890123, null"]],

    ["DATE", ["'1955-11-05', null, '9999-12-31'"]],
    ["DATETIME",
     ["'1955-11-05 01:24:00.000', '9999-12-31 23:59:59.999', null"]],
]

DATA = [
    [([False, True, None],)],
    [([2, None, 255],)],
    [([None, 32767, 256, 7],)],
    [([2147483647, 32768, None, 56],)],
    # Python has dynamic int that has no limit
    [([9223372036854775807, 2147483647, 1, None],)],

    # Discussed inconsistency of REAL and this was accepted
    [([3.1410000324249268, None, 5.315000057220459, 1.0],)],
    [([0.000003, None, 10.000001, 101.000026, 2.3],)],
    [([Decimal('0.12324567890123456789012345678901234567'), None],)],
    [([Decimal('131235.1232'), None],)],

    [([date(1955, 11, 5), None, date(9999, 12, 31)],)],
    [([datetime(1955, 11, 5, 1, 24),
        datetime(9999, 12, 31, 23, 59, 59, 999), None],)],
]


@pytest.mark.parametrize(
    "array_table,data", zip(DATATYPES_DATA, DATA), indirect=['array_table'])
@pytest.mark.usefixtures("array_table")
def test_fetch_array_with_fixed_size(cursor, data):
    """Test simplest portions of each data type of array with fixed size"""
    cursor.execute(f"SELECT data FROM {TEMP_TABLE};")
    result = cursor.fetchall()
    assert result == data


@pytest.mark.parametrize("array_table,data", [
    [
        ["INT", ["1,2,null,3,5", "1,2", "3,76,null", "7"]],
        [([1, 2, None, 3, 5],), ([1, 2],), ([3, 76, None],), ([7],)],
    ],
    [
        ["DATE", ["'1955-11-05',null,'9999-12-31'",
                  "null, '2023-02-19','2020-01-30', '2015-07-03'",
                  "'2010-11-10','1998-10-27'"]],
        [([date(1955, 11, 5), None, date(9999, 12, 31)],),
         ([None, date(2023, 2, 19), date(2020, 1, 30), date(2015, 7, 3)],),
         ([date(2010, 11, 10), date(1998, 10, 27)],)],
    ]
], indirect=['array_table'])
@pytest.mark.usefixtures("array_table")
def test_fetch_array_with_fixed_size_few_rows(cursor, data):
    """Test few rows of array with fixed size"""
    cursor.execute(f"SELECT data FROM {TEMP_TABLE};")
    result = cursor.fetchall()
    assert result == data


def test_fetch_array_with_fixed_size_few_columns(cursor):
    """Test few columns with few rows of array with fixed size"""
    cursor.execute(
        f"CREATE OR REPLACE TABLE {TEMP_TABLE} "
        "(x1 array(INT), x2 array(DOUBLE))")

    cursor.execute(f"SELECT * FROM {TEMP_TABLE};")
    preresult = cursor.fetchall()
    assert preresult == []

    cursor.execute(f"""
        INSERT INTO {TEMP_TABLE} VALUES
        (array(1, 5, null, 10), array(true, false, true, false, null)),
        (null, array(false, false, true)),
        (array(11, 25, 7), null),
        (array(356, 2, 10, 3), array(false, null, true, true))
    """)
    cursor.execute(f"SELECT * FROM {TEMP_TABLE};")
    result = cursor.fetchall()
    assert result == [
        ([1, 5, None, 10], [True, False, True, False, None]),
        (None, [False, False, True]),
        ([11, 25, 7], None),
        ([356, 2, 10, 3], [False, None, True, True]),
    ]

    cursor.execute(f"DROP TABLE {TEMP_TABLE}")


@pytest.mark.parametrize("array_table,data", [
    [
        ["INT", ["1,2,null,4,5,6,7,8", "9,10,11,12,13,14,15,16,17",
                 "18,19,20,21,22,23,24,25,26,27",
                 "1,1,1,1,1,1,1,1, 2,2,2,2,2,2,2,2",
                 "3,3,3,3,3,3,3,3, 4,4,4,4,4,4,4,4, 5",
                 ]],
        [([1, 2, None, 4, 5, 6, 7, 8],),
         ([9, 10, 11, 12, 13, 14, 15, 16, 17],),
         ([18, 19, 20, 21, 22, 23, 24, 25, 26, 27],),
         ([1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2],),
         ([3, 3, 3, 3, 3, 3, 3, 3, 4, 4, 4, 4, 4, 4, 4, 4, 5],),
         ],
    ],
], indirect=['array_table'])
@pytest.mark.usefixtures("array_table")
def test_fetch_array_len_gte8_with_fixed_size(cursor, data):
    """Test array with length greater and equal to 8 (different paddings)"""
    cursor.execute(f"SELECT data FROM {TEMP_TABLE};")
    result = cursor.fetchall()
    assert result == data
