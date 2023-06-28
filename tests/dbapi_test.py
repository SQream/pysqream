"""Tests for network insertion for each data type except ARRAY

Seems that should be a part of testing framework
"""
import struct
import logging
from collections import defaultdict
from datetime import datetime, date
from decimal import Decimal, getcontext
# better use the same standard library, then numpy
from random import randint, uniform, choice
from string import printable

import pytest

import pysqream
from pysqream.cursor import Cursor
from pysqream.errors import ProgrammingError

from .utils import ensure_empty_table, select

logger = logging.getLogger(__name__)


VARCHAR_LENGTH = 10
NVARCHAR_LENGTH = 10
NUMERIC_PRECISION = 38
NUMERIC_SCALE = 10
# Python doesn't have limits on integers, so use SQream specified value
MAX_BIGINT = 9223372036854775807

TEMP_TABLE = "pysqream_dbapi_test_temp"

# Used for transformation of python types to the same returned from SQream
# For example, python's float with double precision, so conversion to single
# precision is required before comparison
COMPARISON_TRANSFORM_FUNCS = defaultdict(
    # Returns function that produce the same value, convenient to avoid if/else
    lambda: lambda x: x,
    {
        'bool': lambda x: bool(x),  # pylint: disable=unnecessary-lambda
        'varchar': lambda x: x.strip(),
        'real': lambda x: struct.unpack('f', struct.pack('f', x))[0],
    }
)


def generate_varchar(length):
    """
    Generate string of size `length` from random printable ASCII chars
    """
    # Should not rely on int representation of letters because it
    # depends on OS
    return ''.join(choice(printable) for _ in range(length))


getcontext().prec = 38


COLUMN_TYPES = [
    'bool', 'tinyint', 'smallint', 'int', 'bigint',
    'real', 'double',
    'date', 'datetime',
    f'varchar({VARCHAR_LENGTH})', f'nvarchar({VARCHAR_LENGTH})',
    f'numeric({NUMERIC_PRECISION},{NUMERIC_SCALE})']

POSITIVE_TEST_VALUES = {
    'bool': (0, 1, True, False, 2, 3.6, 'test', (1997, 5, 9),
             (1997, 12, 12, 10, 10, 10)),
    'tinyint': (randint(0, 255), randint(0, 255), 0, 255, True, False),
    'smallint': (randint(-32768, 32767), 0, -32768, 32767, True, False),
    'int': (randint(-2147483648, 2147483647), 0, -2147483648,
            2147483647, True, False),
    'bigint': (randint(1 - MAX_BIGINT, MAX_BIGINT), 0,
               1 - MAX_BIGINT, MAX_BIGINT, True, False),
    'real': (float('inf'), float('-inf'), float('+0'), float('-0'),
             round(uniform(1e-6, 1e6), 5), 837326.52428, True, False),
    'double': (float('inf'), float('-inf'), float('+0'), float('-0'),
               uniform(1e-6, 1e6), True, False),  # float('nan')
    'date': (date(1998, 9, 24), date(2020, 12, 1), date(1997, 5, 9),
             date(1993, 7, 13), date(1001, 1, 1)),
    'datetime': (datetime(1001, 1, 1, 10, 10, 10),
                 datetime(1997, 11, 30, 10, 10, 10),
                 datetime(1987, 7, 27, 20, 15, 45),
                 datetime(1993, 12, 20, 17, 25, 46)),
    'varchar': (generate_varchar(VARCHAR_LENGTH),
                generate_varchar(VARCHAR_LENGTH),
                generate_varchar(VARCHAR_LENGTH), 'b   '),
    'nvarchar': ('א', 'א  ', '', 'ab א'),
    'numeric': (Decimal("0"), Decimal("1"), Decimal("1.1"),
                Decimal("-1"), Decimal("-1.0"),
                Decimal("12345678901234567890.0123456789"))}

NEGATIVE_TEST_VALUES = {
    'tinyint': (258, 3.6, 'test', (1997, 5, 9), (1997, 12, 12, 10, 10, 10)),
    'smallint': (40000, 3.6, 'test', (1997, 5, 9), (1997, 12, 12, 10, 10, 10)),
    'int': (9999999999, 3.6, 'test', (1997, 5, 9), (1997, 12, 12, 10, 10, 10)),
    'bigint': (92233720368547758070, 3.6, 'test', (1997, 12, 12, 10, 10, 10)),
    'real': ('test', (1997, 12, 12, 10, 10, 10)),
    'double': ('test', (1997, 12, 12, 10, 10, 10)),
    'date': (5, 3.6, (-8, 9, 1), (2012, 15, 6), (2012, 9, 45), 'test',
             False, True),
    'datetime': (5, 3.6, (-8, 9, 1, 0, 0, 0), (2012, 15, 6, 0, 0, 0),
                 (2012, 9, 45, 0, 0, 0), (2012, 9, 14, 26, 0, 0),
                 (2012, 9, 14, 13, 89, 0), 'test', False, True),
    'varchar': (5, 3.6, (1, 2), (1997, 12, 12, 10, 10, 10), False, True),
    'nvarchar': (5, 3.6, (1, 2), (1997, 12, 12, 10, 10, 10), False, True),
    'numeric': ('a')}

# Fill params for parametrization of tests
POSITIVE_TEST_PARAMS = []
NEGATIVE_TEST_PARAMS = []
POSITIVE_TEST_PARAMS_BY_ONE = []
NEGATIVE_TEST_PARAMS_BY_ONE = []
for column_type in COLUMN_TYPES:
    trimmed_type = column_type.split('(', maxsplit=1)[0]
    positive_values = POSITIVE_TEST_VALUES[trimmed_type]
    POSITIVE_TEST_PARAMS.append((column_type, positive_values))
    # add only one value for testing by one
    POSITIVE_TEST_PARAMS_BY_ONE.append((column_type, positive_values[0]))

    if trimmed_type in NEGATIVE_TEST_VALUES:
        negative_values = NEGATIVE_TEST_VALUES[trimmed_type]
        NEGATIVE_TEST_PARAMS.append((column_type, negative_values))
        for neg_val in negative_values:
            NEGATIVE_TEST_PARAMS_BY_ONE.append((column_type, neg_val))

TYPE_TUPLES_FOR_MOCKS = {
    "bool": ['ftBool', 1, 0, 0],
    "tinyint": ['ftUByte', 1, 0, 0],
    "smallint": ['ftShort', 2, 0, 0],
    "int": ['ftInt', 4, 0, 0],
    "bigint": ['ftLong', 8, 0, 0],
    "real": ['ftFloat', 4, 0, 0],
    "double": ['ftDouble', 8, 0, 0],
    "date": ['ftDate', 4, 0, 0],
    "datetime": ['ftDateTime', 8, 0, 0],
    f"varchar({VARCHAR_LENGTH})": ['ftVarchar', VARCHAR_LENGTH, 0, 0],
    f"nvarchar({VARCHAR_LENGTH})": ['ftBlob', 0, 0, 0],
    f"numeric({NUMERIC_PRECISION},{NUMERIC_SCALE})": [
        'ftNumeric', 16, NUMERIC_SCALE, NUMERIC_PRECISION],
}


# TODO: Separate by functionality, not by positive & negative cases
# class TestNetworkInsert:
#     """
#     Test insertion of data via network insert

#     It implies insertion by next flow:
#         >>> data = [
#         ...     (5, "Text1"),  # row 1 (col1, col2)
#         ...     (9, "Text2"),  # row 2
#         ...     ...  # other rows
#         ... ]
#         >>> cursor.executemany("INSERT INTO table VALUES (?, ?)", data)
#         >>> # (?, ?) means values into 2 columns
#     """


# Move those to integration tests
# Logging scenarios from TestBase are not needed anymore, because tests
# are verbose by themselves because of tests names and parametrization
class TestPositive:
    """Test get & set works with valid values"""

    @pytest.mark.slow
    @pytest.mark.parametrize("col_type, data", POSITIVE_TEST_PARAMS)
    def test_positive_insert(self, cursor, col_type, data):
        """Test network insert works while inserting data"""
        ensure_empty_table(cursor, TEMP_TABLE, f"t {col_type}")
        to_insert = [(val,) for val in data]
        cursor.executemany(f"insert into {TEMP_TABLE} values (?)", to_insert)
        res = select(cursor, TEMP_TABLE)

        trimmed_col_type = col_type.split('(', maxsplit=1)[0]
        func = COMPARISON_TRANSFORM_FUNCS[trimmed_col_type]
        to_compare = [(func(val), ) for val in data]
        assert res == to_compare

    @pytest.mark.slow
    @pytest.mark.parametrize("col_type, val", POSITIVE_TEST_PARAMS_BY_ONE)
    def test_positive_insert_by_one(self, cursor, col_type, val):
        """Test network insert works while inserting data by one"""
        ensure_empty_table(cursor, TEMP_TABLE, f"t {col_type}")
        rows = [(val,)]
        cursor.executemany(f"insert into {TEMP_TABLE} values (?)", rows)
        res = select(cursor, TEMP_TABLE)

        trimmed_col_type = col_type.split('(', maxsplit=1)[0]
        to_compare = COMPARISON_TRANSFORM_FUNCS[trimmed_col_type](val)
        assert res == [(to_compare, )]

    @pytest.mark.slow
    @pytest.mark.parametrize("col_type", COLUMN_TYPES)
    def test_positive_insert_nulls_by_one(self, cursor, col_type):
        """Test network insert works while inserting None"""
        # logging does not require because type will be printed with test
        ensure_empty_table(cursor, TEMP_TABLE, f"t {col_type}")
        cursor.executemany(f'insert into {TEMP_TABLE} values (?)', [(None,)])
        res = select(cursor, TEMP_TABLE)
        assert res == [(None, )]

    def test_case_statement_with_nulls(self, cursor):
        """Test network insert works while inserting None"""
        ensure_empty_table(cursor, TEMP_TABLE, "xint int")
        cursor.executemany(f'insert into {TEMP_TABLE} values (?)',
                           [(5,), (None,), (6,), (7,), (None,), (8,), (None,)])
        cursor.executemany(f"select case when xint is null then 1 else 0 end "
                           f"from {TEMP_TABLE}")
        expected_list = [0, 1, 0, 0, 1, 0, 1]
        res_list = [x[0] for x in cursor.fetchall()]
        assert expected_list == res_list

    @pytest.mark.parametrize('value', (True, False))
    def test_select_bool_literals(self, cursor, value):
        """Test select true/false returns integer value"""
        cursor.execute(f"select {value}")
        res = cursor.fetchall()
        assert res == [(int(value), )]

    def test_running_statement_while_open_statement(self, cursor):
        """Open statement should not impact new statement"""
        cursor.execute("select 1")
        # don't need to sleep, it's only slowing tests.  Code is synchronous.
        res = cursor.execute("select 2").fetchall()  # Change value to be sure
        assert res == [(2, )]


@pytest.mark.mock_connection
class TestNegativeGetSetWithMockConnection:
    """
    Negative Set/Get tests using mocked connection

    Real connection is no required for this tests, could use mocks,
    because actual errors are raised by validation at pysqream side
    """

    @pytest.fixture
    def mock_cursor_exec(self, mock_cursor, monkeypatch):
        """
        Mocks execute method of cursor, for those checks errors after it
        Use global fixture mock_cursor which does not create real connection,
        so works faster
        """
        monkeypatch.setattr(mock_cursor, 'execute', lambda x: None)
        yield mock_cursor

    def assert_insert_raises_pack(self, cursor, table_name: str, data):
        """
        Asserts that insertion of data into table_name raises ProgrammingError
        """
        __tracebackhide__ = True  # pylint: disable=unused-variable
        self._assert_insert_raises(
            cursor, table_name, data, ProgrammingError,
            "Error packing columns. Check that all types match the respective "
            "column types"
        )

    @staticmethod
    def _assert_insert_raises(
            cursor, table_name: str, data,
            exception=ProgrammingError, error_msg=""
    ):
        with pytest.raises(exception, match=error_msg):
            cursor.executemany(f"insert into {table_name} values (?)", data)

    @pytest.mark.parametrize("col_type, data", NEGATIVE_TEST_PARAMS)
    def test_negative_insert(self, mock_cursor_exec, col_type, data):
        """Test that insertion of bunch of invalid values raises"""
        mock_col_metadata_for_column(mock_cursor_exec, col_type)
        to_insert = [(value, ) for value in data]
        self.assert_insert_raises_pack(mock_cursor_exec, TEMP_TABLE, to_insert)

    @pytest.mark.parametrize("col_type, value", NEGATIVE_TEST_PARAMS_BY_ONE)
    def test_negative_insert_by_one(self, mock_cursor_exec, col_type, value):
        """Test that insertion of single invalid value raises"""
        mock_col_metadata_for_column(mock_cursor_exec, col_type)
        data = [(value,)]
        self.assert_insert_raises_pack(mock_cursor_exec, TEMP_TABLE, data)

    def test_inconsistent_sizes(self, mock_cursor_exec):
        """Test inconsistent insertion data """
        error_msg = "Incosistent data sequences passed for inserting. Please "\
                    "use rows/columns of consistent length"
        data = [(5,), (6, 9), (7, 8)]
        with pytest.raises(ProgrammingError, match=error_msg):
            mock_cursor_exec.executemany(
                f'insert into {TEMP_TABLE} values (?, ?)', data)

    @pytest.mark.parametrize("stmt", ["DML", "INSERT"])
    def test_incorrect_fetchmany_wrong_statement(self, mock_cursor_exec, stmt):
        """fetchmany is allowed only after SELECT statement"""
        mock_cursor_exec.statement_type = stmt
        with pytest.raises(
                ProgrammingError,
                match="No open statement while attempting fetch operation"):
            mock_cursor_exec.fetchmany(2)

    @pytest.mark.parametrize("method", ["fetchall", "fetchone"])
    def test_incorrect_fetch_all_one_args(self, mock_cursor_exec, method):
        """
        fetchall/fetchone does not support any arguments except "rows"
        """
        func = getattr(mock_cursor_exec, method)
        with pytest.raises(Exception, match="Bad argument to fetch(all|one)"):
            func(5)

    def test_parametered_query(self, mock_cursor):
        """Parametered queries not supported by pysqream"""
        with pytest.raises(
                Exception, match="Parametered queries not supported"):
            mock_cursor.execute('select * from test where xint > ?', "6")

    def test_execute_closed_cursor(self, mock_cursor):
        """Attempt to execute on closed cursor raises ProgrammingError"""
        mock_cursor.close()
        with pytest.raises(ProgrammingError, match="Cursor has been closed"):
            mock_cursor.execute("select 1")


class TestNegative:
    """Negative Set/Get for DB responses"""

    @pytest.mark.parametrize("char_type, error", [
        ("varchar", "Conversion of a varchar to a smaller length is not sup"),
        ("nvarchar", "is too long for column")
    ])
    def test_varchars_conversion_not_supported(self, cursor, char_type, error):
        """
        Conversion of a (n)varchar to a smaller length is not supported by DB
        """
        ensure_empty_table(cursor, TEMP_TABLE, f"data {char_type}(10)")
        with pytest.raises(Exception, match=error):
            cursor.executemany(
                f"insert into {TEMP_TABLE} values ('aa12345678910')")

    def test_multi_statement_are_not_supported(self, cursor):
        """DB does not support multiple statement in one execution"""
        with pytest.raises(Exception, match="expected one statement, got"):
            cursor.execute("select 1; select 1;")


@pytest.mark.mock_connection
class TestFetch:
    """Test fetch methods using mocked connection"""

    @pytest.fixture
    def cursor_with_data(self, monkeypatch, mock_cursor):
        """
        Fixture that set range 1 - 11 on call Cursor._fetch_and_parse

        All fetch methods call _fetch_and_parse under the hood, where the
        minimum requested data depends on the server response and does not
        depend on requested row amount (until it is greater)
        """

        def m_execute(obj, *_, **__):
            """Mock for execute to reset parsed_rows"""
            obj.more_to_fetch = True
            obj.parsed_rows = []

        monkeypatch.setattr(mock_cursor, '_fetch', lambda *_: False)
        monkeypatch.setattr(
            mock_cursor, '_parse_fetched_cols', lambda *_: [range(1, 11)])
        monkeypatch.setattr(Cursor, 'execute', m_execute)
        yield mock_cursor

    def test_consistent_results_fetchone_fetchmany(self, cursor_with_data):
        """fetchmany(1) vs fetchone() should produce consistent results"""
        cursor_with_data.execute(f"select * from {TEMP_TABLE}")
        res = cursor_with_data.fetchmany(1)[0][0]
        cursor_with_data.execute(f"select * from {TEMP_TABLE}")
        res2 = cursor_with_data.fetchone()[0]
        assert res == res2 == 1

    def test_consistent_results_fetchall_fetchmany(self, cursor_with_data):
        """fetchmany(-1) vs fetchall() should produce consistent results"""
        cursor_with_data.execute(f"select * from {TEMP_TABLE}")
        res = cursor_with_data.fetchmany(-1)
        cursor_with_data.execute(f"select * from {TEMP_TABLE}")
        res2 = cursor_with_data.fetchall()
        assert res == res2

    def test_fetchone_loop(self, cursor_with_data):
        """fetchone() should continue retrieve the data"""
        cursor_with_data.execute(f"select * from {TEMP_TABLE}")
        for i in range(1, 11):
            assert cursor_with_data.fetchone()[0] == i

    def test_combined_fetch(self, cursor_with_data):
        """fetchone() and fetchmany(custom_value) should continue retrieval"""
        cursor_with_data.execute(f"select * from {TEMP_TABLE}")
        res_list = [cursor_with_data.fetchone()[0]]
        assert res_list == [1]
        res_list += [x[0] for x in cursor_with_data.fetchmany(2)]
        assert res_list == [1, 2, 3]
        res_list.append(cursor_with_data.fetchone()[0])
        assert res_list == [1, 2, 3, 4]
        res_list += [x[0] for x in cursor_with_data.fetchall()]
        assert res_list == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

    def test_fetch_after_data_read(self, cursor_with_data):
        """Test data is still """
        cursor_with_data.execute("select * from test")
        res = cursor_with_data.fetchmany(9)
        assert res == [(v, ) for v in range(1, 10)]
        res2 = cursor_with_data.fetchone()[0]
        assert res2 == 10
        res3 = cursor_with_data.fetchone()
        assert res3 is None

        res4 = cursor_with_data.fetchall()
        assert res4 == []

        res5 = cursor_with_data.fetchmany(1)
        assert res5 == []


class TestCursor:
    """Tests for pysqream.cursor.Cursor"""
    # TODO: Move in tests/cursor folder

    def test_cursor_through_clustered(self, ip_address, cport):
        """Test for cursor working with clustered connection"""
        # TODO: should be tested not from Cursor, but as part of
        # Connection._open_connection
        con_clustered = pysqream.connect(
            ip_address, cport, 'master', 'sqream', 'sqream', clustered=True)
        cur = con_clustered.cursor()
        assert cur.execute("select 1").fetchall()[0][0] == 1
        cur.close()
        con_clustered.close()

    @pytest.mark.slow
    def test_two_statements_same_cursor(self, cursor):
        """Test two statements work using the same cursor"""
        # Seems to be tested while others tests runs
        res1 = cursor.execute("select 1").fetchall()[0][0]
        assert res1 == 1
        res2 = cursor.execute("select 2").fetchall()[0][0]
        assert res2 == 2

    # Duplcated tests removed here:
    # test_cursor_when_open_statement was tested
    # in test_running_statement_while_open_statement

    # test_fetch_after_all_read was tested in test_fetch_after_data_read


# TestString is tested while testing of all types, including special symbols
# But seems should be part of testing of converters


def mock_col_metadata_for_column(cursor: Cursor, col_type: str):
    """
    Adds col_types, col_sizes, col_scales, col_nul, col_tvc to cursor
    """
    ft_type, size, scale, _ = TYPE_TUPLES_FOR_MOCKS[col_type]
    # mocking column metadata retrieving. monkeypatch is not required,
    # because those attributes are reset each execute
    cursor.col_types = [ft_type]
    cursor.col_sizes = [size]
    cursor.col_scales = [scale]
    cursor.col_nul = [True]
    cursor.col_tvc = ["nvarchar" in col_type]
