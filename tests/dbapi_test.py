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

from .base import TestBase, Logger, connect_dbapi
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


col_types = [
    'bool', 'tinyint', 'smallint', 'int', 'bigint',
    'real', 'double',
    'date', 'datetime',
    f'varchar({VARCHAR_LENGTH})', f'nvarchar({VARCHAR_LENGTH})',
    f'numeric({NUMERIC_PRECISION},{NUMERIC_SCALE})']

pos_test_vals = {
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

neg_test_vals = {
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
POSITIVE_TEST_PARAMS_BY_ONE = []
for type_ in col_types:
    values = pos_test_vals[type_.split('(', maxsplit=1)[0]]
    POSITIVE_TEST_PARAMS.append((type_, values))
    # add only one value for testing by one
    POSITIVE_TEST_PARAMS_BY_ONE.append((type_, values[0]))


# TODO: Separate by functionality, not by positive & negative cases
# class TestNetworkInsert(TestBase):
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


# Logging scenarios from TestBase are not needed anymore, because tests
# are verbose by themselves because of tests names and parametrization
class TestPositive:
    """Test get & set works with valid values"""

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

    @pytest.mark.parametrize("col_type", col_types)
    def test_positive_insert_nulls_by_one(self, cursor, col_type):
        """Test network insert works while inserting None"""
        # logging does not require because type will be printed with test
        ensure_empty_table(cursor, TEMP_TABLE, f"t {col_type}")
        cursor.executemany(f'insert into {TEMP_TABLE} values (?)', [(None,)])
        res = select(cursor, TEMP_TABLE)
        assert res == [(None, )]
        cursor.close()

    def test_case_statement_with_nulls(self, cursor):
        """Test network insert works while inserting None"""
        ensure_empty_table(cursor, TEMP_TABLE, "xint int")
        cursor.executemany(f'insert into {TEMP_TABLE} values (?)',
                           [(5,), (None,), (6,), (7,), (None,), (8,), (None,)])
        cursor.executemany(f"select case when xint is null then 1 else 0 end "
                           f"from {TEMP_TABLE}")
        expected_list = [0, 1, 0, 0, 1, 0, 1]
        res_list = [x[0] for x in cursor.fetchall()]
        cursor.close()
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
        cursor.close()
        assert res == [(2, )]


class TestNegative(TestBase):
    ''' Negative Set/Get tests '''

    def test_negative(self):

        cur = self.con.cursor()
        Logger().info('Negative tests')
        for col_type in col_types:
            Logger().debug("Col type: %s", col_type)
            if col_type == 'bool':
                continue
            trimmed_col_type = col_type.split('(')[0]
            cur.execute("create or replace table test (t_{} {})".format(trimmed_col_type, col_type))
            for val in neg_test_vals[trimmed_col_type]:
                rows = [(val,)]
                with pytest.raises(Exception) as e:
                    cur.executemany("insert into test values (?)", rows)
                assert "Error packing columns. Check that all types match the respective column types" in str(e.value)
        cur.close()

    def test_incosistent_sizes(self):

        cur = self.con.cursor()
        Logger().info("Inconsistent sizes test")
        cur.execute("create or replace table test (xint int, yint int)")
        with pytest.raises(Exception) as e:
            cur.executemany('insert into test values (?, ?)', [(5,), (6, 9), (7, 8)])
        cur.close()
        assert "Incosistent data sequences passed for inserting. Please use rows/columns of consistent length" in str(
            e.value)

    def test_varchar_conversion(self):

        cur = self.con.cursor()
        Logger().info("Varchar - Conversion of a varchar to a smaller length")
        cur.execute("create or replace table test (test varchar(10))")
        with pytest.raises(Exception) as e:
            cur.executemany("insert into test values ('aa12345678910')")
        cur.close()
        assert "expected response statementPrepared but got" in str(e.value)

    def test_nvarchar_conversion(self):

        cur = self.con.cursor()
        Logger().info("Nvarchar - Conversion of a varchar to a smaller length")
        cur.execute("create or replace table test (test nvarchar(10))")
        with pytest.raises(Exception) as e:
            cur.executemany("insert into test values ('aa12345678910')")
        cur.close()
        assert "expected response executed but got" in str(e.value)

    def test_incorrect_fetchmany(self):

        cur = self.con.cursor()
        Logger().info("Incorrect usage of fetchmany - fetch without a statement")
        cur.execute("create or replace table test (xint int)")
        with pytest.raises(Exception) as e:
            cur.fetchmany(2)
        cur.close()
        assert "No open statement while attempting fetch operation" in str(e.value)

    def test_incorrect_fetchall(self):

        cur = self.con.cursor()
        Logger().info("Incorrect usage of fetchall")
        cur.execute("create or replace table test (xint int)")
        cur.executemany("select * from test")
        with pytest.raises(Exception) as e:
            cur.fetchall(5)
        cur.close()
        assert "Bad argument to fetchall" in str(e.value)

    def test_incorrect_fetchone(self):

        cur = self.con.cursor()
        Logger().info("Incorrect usage of fetchone")
        cur.execute("create or replace table test (xint int)")
        cur.executemany("select * from test")
        with pytest.raises(Exception) as e:
            cur.fetchone(5)
        cur.close()
        assert "Bad argument to fetchone" in str(e.value)

    def test_multi_statement(self):

        cur = self.con.cursor()
        Logger().info("Multi statements test")
        with pytest.raises(Exception) as e:
            cur.execute("select 1; select 1;")
        cur.close()
        assert "expected one statement, got" in str(e.value)

    def test_parametered_query(self):

        cur = self.con.cursor()
        Logger().info("Parametered query tests")
        params = 6
        cur.execute("create or replace table test (xint int)")
        cur.executemany('insert into test values (?)', [(5,), (6,), (7,)])
        with pytest.raises(Exception) as e:
            cur.execute('select * from test where xint > ?', str(params))
        cur.close()
        assert "Parametered queries not supported" in str(e.value)

    def test_execute_closed_cursor(self):

        cur = self.con.cursor()
        Logger().info("running execute on a closed cursor")
        cur.close()
        try:
            cur.execute("select 1")
        except Exception as e:
            if "Cursor has been closed" not in repr(e):
                raise Exception(f'bad error message')


class TestFetch(TestBase):

    def test_fetch(self):

        cur = self.con.cursor()
        cur.execute("create or replace table test (xint int)")
        cur.executemany('insert into test values (?)', [(1,), (2,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,)])
        # fetchmany(1) vs fetchone()
        cur.execute("select * from test")
        res = cur.fetchmany(1)[0][0]
        cur.execute("select * from test")
        res2 = cur.fetchone()[0]
        cur.close()
        assert res == res2

        # fetchmany(-1) vs fetchall()
        cur = self.con.cursor()
        cur.execute("select * from test")
        res3 = cur.fetchmany(-1)
        cur.execute("select * from test")
        res4 = cur.fetchall()
        cur.close()
        assert res3 == res4

        # fetchone() loop
        cur = self.con.cursor()
        cur.execute("select * from test")
        for i in range(1, 11):
            x = cur.fetchone()[0]
            assert x == i
        cur.close()

    def test_combined_fetch(self):

        cur = self.con.cursor()
        cur.execute("create or replace table test (xint int)")
        cur.executemany('insert into test values (?)', [(1,), (2,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,)])
        cur.execute("select * from test")
        expected_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        res_list = []
        res_list.append(cur.fetchone()[0])
        res_list += [x[0] for x in cur.fetchmany(2)]
        res_list.append(cur.fetchone()[0])
        res_list += [x[0] for x in cur.fetchall()]
        cur.close()
        assert expected_list == res_list

    def test_fetch_after_data_read(self):

        cur = self.con.cursor()
        cur.execute("create or replace table test (xint int)")
        cur.executemany('insert into test values (?)', [(1,)])
        cur.execute("select * from test")
        x = cur.fetchone()[0]
        res = cur.fetchone()
        assert res is None

        res = cur.fetchall()
        assert res == []

        res = cur.fetchmany(1)
        assert res == []

        cur.close()


class TestCursor:
    def test_cursor_through_clustered(self, ip_address):
        con_clustered = pysqream.connect(
            ip_address, 3108, 'master', 'sqream', 'sqream', clustered=True)
        cur = con_clustered.cursor()
        assert cur.execute("select 1").fetchall()[0][0] == 1
        cur.close()

    def test_two_statements_same_cursor(self, ip_address):
        vals = [1]
        con = connect_dbapi(ip_address)
        cur = con.cursor()
        cur.execute("select 1")
        res1 = cur.fetchall()[0][0]
        vals.append(res1)
        cur.execute("select 1")
        res2 = cur.fetchall()[0][0]
        vals.append(res2)
        cur.close()
        con.close()
        assert all(x == vals[0] for x in vals)

    def test_cursor_when_open_statement(self, ip_address):
        con = connect_dbapi(ip_address)
        cur = con.cursor()
        cur.execute("select 1")
        # don't need to sleep, it's only slowing tests.  Code is synchronous.
        cur.execute("select 1")
        res = cur.fetchall()[0][0]
        cur.close()
        con.close()
        assert res == 1

    def test_fetch_after_all_read(self, ip_address):
        con = connect_dbapi(ip_address)
        cur = con.cursor()
        cur.execute("create or replace table test (xint int)")
        cur.executemany('insert into test values (?)', [(1,)])
        cur.execute("select * from test")
        x = cur.fetchone()[0]
        res = cur.fetchone()
        assert res is None

        res = cur.fetchall()
        assert res == []

        res = cur.fetchmany(1)
        assert res == []
        cur.close()
        con.close()


class TestString(TestBase):

    def test_insert_return_utf8(self):
        cur = self.con.cursor()
        cur.execute("create or replace table test (xvarchar varchar(20))")
        cur.executemany('insert into test values (?)', [(u"hello world",), ("hello world",)])
        cur.execute("select * from test")
        res = cur.fetchall()
        cur.close()
        assert res[0][0] == res[1][0]

    def test_strings_with_escaped_chars(self):
        cur = self.con.cursor()
        cur.execute("create or replace table test (xvarchar varchar(20))")
        values = [("\t",), ("\n",), ("\\n",), ("\\\n",), (" \\",), ("\\\\",), (" \nt",), ("'abd''ef'",), ("abd""ef",),
                  ("abd\"ef",)]
        cur.executemany('insert into test values (?)', values)
        cur.executemany("select * from test")
        expected_list = ['', '', '\\n', '\\', ' \\', '\\\\', ' \nt', "'abd''ef'", 'abdef', 'abd"ef']
        res_list = []
        res_list += [x[0] for x in cur.fetchall()]
        cur.close()
        assert expected_list == res_list