"""Test fetching SQREAM Arrays of all datatype"""
from datetime import date, datetime
from decimal import Decimal
from string import ascii_lowercase, ascii_uppercase

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


def str_to_decimal(value: str):
    """Utility to transform string to decimal.Decimal"""
    if value == 'null':
        return None
    return Decimal(value)


def tuple_to_datetime(values: tuple):
    """Utility to tuple of values to datetime"""
    if values is None:
        return None
    return datetime(*values)


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

TEXT_VALUES = [
    [None, "Kiwis have tiny wings, but cannot fly.", "", "xXx"],
    ["ABCDEF", ascii_lowercase, None, ascii_uppercase],
    # 8 or more letters in array
    ["ABCDEFGH", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", "B"],
    # 8 or more elements in array:
    [ascii_lowercase[:8], ascii_lowercase[8:17], "A", "B", "C", "D", "E", "F"],
    [ascii_lowercase[:9], ascii_lowercase[9:17],
     "A", "B", "C", "D", "E", "F", "G"],
    # Other unicode symbols:
    ["Привет", "세계", '!', "мир", "חֲבֵרוּת", "халықтар", "فن", "将来の技術"],
    # emoji
    ["😀😁😊", "🤩😝", "🌈 Rainbow 🦄"],
]

TEXT_INSERT_VALUES = [
    'null' if row is None else ', '.join(
        ['null' if val is None else f"'{val}'" for val in row])
    for row in TEXT_VALUES
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


NUMERICS_GTE8 = [
    ("0.12324567890123456789012345678901234567,null,"
     "0.73270933514729256060654459060436146320,"
     "0.27710910203621938654256711973474258656,"
     "0.87027550286696898297392774332314238215,"
     "0.28344642067447480418303137230477094303,"
     "0.84439463739580009186975395341855804715,"
     "0.72050074750794499867189301351808176034"),
    # Issue with inserting 2 rows of array with NUMERIC in sqreamDB
    # ("0.89008116931860775456438745519952355797,"
    #  "0.97890877230628508281462449166292548787,"
    #  "0.28598153938468105230586792195041658405,"
    #  "0.56962502909270102011674900745166157417,"
    #  "0.50071372140732800052840762804855954372,"
    #  "0.10761565282959626734609215196665492084,"
    #  "0.88563821976143413830211310495967218854,"
    #  "0.97697038294893417155145140125394210678,"
    #  "0.5229561858036450176084567199944221919"),
]

DATETIME_GTE8 = [
    [tuple_to_datetime(vals) for vals in
        [(1955, 11, 5, 1, 24), None, (9999, 12, 31, 23, 59, 59, 999), None,
         (5245, 8, 19, 7, 13, 16, 250), (2020, 5, 22, 15, 21, 51, 772),
         None, (1998, 1, 1)]],
    [tuple_to_datetime(vals) for vals in
        [(3657, 6, 3, 15, 7, 38, 150), (6841, 3, 27, 17, 10, 45, 311),
         None, None, (1945, 10, 4, 21, 3, 7, 315),
         (2154, 9, 1, 9, 00, 30, 150), None, (1998, 1, 1),
         (5000, 5, 5, 5, 5, 55, 555)]]
]


DATATYPES_DATA_GTE8 = [
    ["BOOL", ["false, true, null, false, true, true, true, true",
              "false, false, false, true, false, null, true, true, true"]],
    ["TINYINT", ["2, null, 255, 255, 246, 93, 168, 2",
                 "90, 68, 233, 22, 115, 101, 230, 187, 238"]],
    ["SMALLINT", ["null, 32767, 256, 7, 739, 11750, 18377, 17064, 70",
                  "30629, 1568, null, 19321, 26685, 3203, 21480, 27665"]],
    ["INT", ["1, 2, null, 4, 5, 6, 7, 8", "9, 10, 11, 12, 13, 14, 15, 16, 17",
             "18, 19, 20, 21, 22, 23, 24, 25, 26, 27",
             "1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2",
             "3, 3, 3, 3, 3, 3, 3, 3, 4, 4, 4, 4, 4, 4, 4, 4, 5",
             ]],
    ["BIGINT",
     ["9223372036854775807, 2147483647, 1, null, 524144892539322350,"
      "148882015931100541, 3739342555072671632, 4646279364338702782",

      "2886478542906251506, 379035858234911484, 7003546590056584421,"
      "7702409819336849648, 6696807386994580137, 1011291306131098677,"
      "1004283815390051765, 1165666090405286343, 2046035832239837009"]],

    ["REAL",
     ["3.141, null, 5.315, 1.0, -227.40676225642, -47.62984,"
      "240.20508239175058, -12.335671917643026",
      "-123.69, 159.744806241, 39.529048, 167.520499486, -232.0,"
      "147.73126662588345, -53.89, -12.27767032678"]],
    ["DOUBLE",
     ["0.000003, null, 10.000001, 101.000026, 2.3, -227.40676225642, "
      "-47.62984, 240.20508239175058, -12.335671917643026",
      "-123.69, 159.744806241, 39.529048, 167.520499486, -232.0,"
      "147.73126662588345, -53.89, -12.27767032678"]],
    ["NUMERIC(38,38)", NUMERICS_GTE8],

    ["DATE",
     [','.join([d.strftime("'%F'") if d else "null" for d in r])
      for r in DATETIME_GTE8]],
    ["DATETIME",
     # There is an issue when sqream response using time with ms,
     # but pysqream passes it to datetime as microseconds
     # So 2015-05-23 15:48:25.564 should be:
     # datetime(2015, 05, 23, 15, 45, 25, 564000), but it 564 which
     # represent value of 2015-05-23 15:48:25.000564
     # After discussion with danielg decided to pass it as it for
     # compatibility
     [','.join([f"'{d.strftime('%F %H:%M:%S')}.{d.microsecond*1000:03.0f}'"
                if d else "null" for d in r])
      for r in DATETIME_GTE8]],
]


DATA_GTE8 = [
    [([False, True, None, False, True, True, True, True],),
     ([False, False, False, True, False, None, True, True, True],)],
    [([2, None, 255, 255, 246, 93, 168, 2],),
     ([90, 68, 233, 22, 115, 101, 230, 187, 238],)],
    [([None, 32767, 256, 7, 739, 11750, 18377, 17064, 70],),
     ([30629, 1568, None, 19321, 26685, 3203, 21480, 27665],)],
    [([1, 2, None, 4, 5, 6, 7, 8],),
     ([9, 10, 11, 12, 13, 14, 15, 16, 17],),
     ([18, 19, 20, 21, 22, 23, 24, 25, 26, 27],),
     ([1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2],),
     ([3, 3, 3, 3, 3, 3, 3, 3, 4, 4, 4, 4, 4, 4, 4, 4, 5],),
     ],
    [([9223372036854775807, 2147483647, 1, None, 524144892539322350,
       148882015931100541, 3739342555072671632, 4646279364338702782],),
     ([2886478542906251506, 379035858234911484, 7003546590056584421,
       7702409819336849648, 6696807386994580137, 1011291306131098677,
       1004283815390051765, 1165666090405286343, 2046035832239837009],)],

    [([3.1410000324249268, None, 5.315000057220459, 1.0, -227.40676879882812,
       -47.62984085083008, 240.205078125, -12.335672378540039],),
     ([-123.69000244140625, 159.74481201171875, 39.529048919677734,
       167.52049255371094, -232.0, 147.73126220703125, -53.88999938964844,
       -12.277669906616211],)],
    [([0.000003, None, 10.000001, 101.000026, 2.3, -227.40676225642, -47.62984,
       240.20508239175058, -12.335671917643026],),
     ([-123.69, 159.744806241, 39.529048, 167.520499486, -232.0,
       147.73126662588345, -53.89, -12.27767032678],)],
    [([str_to_decimal(val) for val in r.split(',')],) for r in NUMERICS_GTE8],

    [([d.date() if d else None for d in row],) for row in DATETIME_GTE8],
    [(row,) for row in DATETIME_GTE8],
]


@pytest.mark.parametrize(
    "array_table,data",
    zip(DATATYPES_DATA_GTE8, DATA_GTE8),
    indirect=['array_table'])
@pytest.mark.usefixtures("array_table")
def test_fetch_array_len_gte8_with_fixed_size(cursor, data):
    """Test array with length greater and equal to 8 (different paddings)"""
    cursor.execute(f"SELECT data FROM {TEMP_TABLE};")
    result = cursor.fetchall()
    assert result == data


@pytest.mark.parametrize("array_table,data", [
    [
        ["TEXT", TEXT_INSERT_VALUES],
        [(t,) for t in TEXT_VALUES],
    ],
], indirect=['array_table'])
@pytest.mark.usefixtures("array_table")
def test_fetch_array_unfixed_size(cursor, data):
    """Test array with unfixed sise - TEXT"""
    cursor.execute(f"SELECT data FROM {TEMP_TABLE};")
    result = cursor.fetchall()
    assert result == data
