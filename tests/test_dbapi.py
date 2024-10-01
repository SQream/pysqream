import sys
from datetime import datetime, date
from decimal import Decimal, getcontext
from numpy.random import randint, uniform
from queue import Queue
from time import sleep

import pysqream
import pytest

from tests.test_base import TestBase, TestBaseWithoutBeforeAfter, Logger, connect_dbapi


q = Queue()
logger = Logger()
varchar_length = 10
nvarchar_length = 10
precision = 38
scale = 10
max_bigint = sys.maxsize if sys.platform not in ('win32', 'cygwin') else 2147483647


def generate_varchar(length):
    return ''.join(chr(num) for num in randint(32, 128, length))


getcontext().prec = 38


col_types = ['bool', 'tinyint', 'smallint', 'int', 'bigint', 'real', 'double', 'date', 'datetime',
             'varchar({})'.format(varchar_length), 'nvarchar({})'.format(varchar_length),
             'numeric({},{})'.format(precision, scale)]

pos_test_vals = {'bool': (0, 1, True, False, 2, 3.6, 'test', (1997, 5, 9), (1997, 12, 12, 10, 10, 10)),
                 'tinyint': (randint(0, 255), randint(0, 255), 0, 255, True, False),
                 'smallint': (randint(-32768, 32767), 0, -32768, 32767, True, False),
                 'int': (randint(-2147483648, 2147483647), 0, -2147483648, 2147483647, True, False),
                 'bigint': (randint(1 - max_bigint, max_bigint), 0, 1 - max_bigint, max_bigint, True, False),
                 'real': (
                 float('inf'), float('-inf'), float('+0'), float('-0'), round(uniform(1e-6, 1e6), 5), 837326.52428,
                 True, False),  # float('nan')
                 'double': (float('inf'), float('-inf'), float('+0'), float('-0'), uniform(1e-6, 1e6), True, False),
                 # float('nan')
                 'date': (date(1998, 9, 24), date(2020, 12, 1), date(1997, 5, 9), date(1993, 7, 13), date(1001, 1, 1)),
                 'datetime': (datetime(1001, 1, 1, 10, 10, 10), datetime(1997, 11, 30, 10, 10, 10),
                              datetime(1987, 7, 27, 20, 15, 45), datetime(1993, 12, 20, 17, 25, 46)),
                 'varchar': (
                 generate_varchar(varchar_length), generate_varchar(varchar_length), generate_varchar(varchar_length),
                 'b   '),
                 'nvarchar': ('א', 'א  ', '', 'ab א'),
                 'numeric': (Decimal("0"), Decimal("1"), Decimal("1.1"), Decimal("-1"), Decimal("-1.0"),
                             Decimal("12345678901234567890.0123456789"))}

neg_test_vals = {'tinyint': (258, 3.6, 'test', (1997, 5, 9), (1997, 12, 12, 10, 10, 10)),
                 'smallint': (40000, 3.6, 'test', (1997, 5, 9), (1997, 12, 12, 10, 10, 10)),
                 'int': (9999999999, 3.6, 'test', (1997, 5, 9), (1997, 12, 12, 10, 10, 10)),
                 'bigint': (92233720368547758070, 3.6, 'test', (1997, 12, 12, 10, 10, 10)),
                 'real': ('test', (1997, 12, 12, 10, 10, 10)),
                 'double': ('test', (1997, 12, 12, 10, 10, 10)),
                 'date': (5, 3.6, (-8, 9, 1), (2012, 15, 6), (2012, 9, 45), 'test', False, True),
                 'datetime': (
                 5, 3.6, (-8, 9, 1, 0, 0, 0), (2012, 15, 6, 0, 0, 0), (2012, 9, 45, 0, 0, 0), (2012, 9, 14, 26, 0, 0),
                 (2012, 9, 14, 13, 89, 0), 'test', False, True),
                 'varchar': (5, 3.6, (1, 2), (1997, 12, 12, 10, 10, 10), False, True),
                 'nvarchar': (5, 3.6, (1, 2), (1997, 12, 12, 10, 10, 10), False, True),
                 'numeric': ('a')}


class TestPositive(TestBase):

    def test_positive(self):

        cur = self.con.cursor()
        logger.info('positive tests')
        for col_type in col_types:
            trimmed_col_type = col_type.split('(')[0]

            logger.info(f'Inserted values test for column type {col_type}')
            cur.execute(f"create or replace table test (t_{trimmed_col_type} {col_type})")
            for val in pos_test_vals[trimmed_col_type]:
                cur.execute('truncate table test')
                rows = [(val,)]
                cur.executemany("insert into test values (?)", rows)
                res = cur.execute("select * from test").fetchall()[0][0]
                # Compare
                error = False
                assert (
                        val == res or
                        (val != res and trimmed_col_type == 'bool' and val != 0 and res == True) or
                        (val != res and trimmed_col_type == 'varchar' and val != 0 and val.strip() == res) or
                        (val != res and trimmed_col_type == 'real' and val != 0 and abs(res - val) <= 0.1)
                )

            logger.info(f'Null test for column type: {col_type}')
            cur.execute("create or replace table test (t_{} {})".format(trimmed_col_type, col_type))
            cur.executemany('insert into test values (?)', [(None,)])
            res = cur.execute('select * from test').fetchall()[0][0]
            assert res == None

        cur.close()

    def test_nulls(self):

        cur = self.con.cursor()
        logger.info("Case statement with nulls")
        cur.execute("create or replace table test (xint int)")
        cur.executemany('insert into test values (?)', [(5,), (None,), (6,), (7,), (None,), (8,), (None,)])
        cur.executemany("select case when xint is null then 1 else 0 end from test")
        expected_list = [0, 1, 0, 0, 1, 0, 1]
        res_list = []
        res_list += [x[0] for x in cur.fetchall()]
        cur.close()
        assert expected_list == res_list

    def test_bool(self):

        cur = self.con.cursor()
        logger.info("Testing select true/false")
        cur.execute("select false")
        res = cur.fetchall()[0][0]
        assert res == 0

        cur.execute("select true")
        res = cur.fetchall()[0][0]
        cur.close()
        assert res == 1

    def test_when_running(self):

        cur = self.con.cursor()
        logger.info("Running a statement when there is an open statement")
        cur.execute("select 1")
        sleep(10)
        res = cur.execute("select 1").fetchall()[0][0]
        cur.close()
        assert res == 1


class TestNegative(TestBase):
    ''' Negative Set/Get tests '''

    def test_negative(self):

        cur = self.con.cursor()
        logger.info('Negative tests')
        for col_type in col_types:
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
        logger.info("Inconsistent sizes test")
        cur.execute("create or replace table test (xint int, yint int)")
        with pytest.raises(Exception) as e:
            cur.executemany('insert into test values (?, ?)', [(5,), (6, 9), (7, 8)])
        cur.close()
        assert "Incosistent data sequences passed for inserting. Please use rows/columns of consistent length" in str(
            e.value)

    def test_varchar_conversion(self):

        cur = self.con.cursor()
        logger.info("Varchar - Conversion of a varchar to a smaller length")
        cur.execute("create or replace table test (test varchar(10))")
        with pytest.raises(Exception) as e:
            cur.executemany("insert into test values ('aa12345678910')")
        cur.close()
        assert "expected response statementPrepared but got" in str(e.value)

    def test_nvarchar_conversion(self):

        cur = self.con.cursor()
        logger.info("Nvarchar - Conversion of a varchar to a smaller length")
        cur.execute("create or replace table test (test nvarchar(10))")
        with pytest.raises(Exception) as e:
            cur.executemany("insert into test values ('aa12345678910')")
        cur.close()
        assert "expected response executed but got" in str(e.value)

    def test_incorrect_fetchmany(self):

        cur = self.con.cursor()
        logger.info("Incorrect usage of fetchmany - fetch without a statement")
        cur.execute("create or replace table test (xint int)")
        with pytest.raises(Exception) as e:
            cur.fetchmany(2)
        cur.close()
        assert "No open statement while attempting fetch operation" in str(e.value)

    def test_incorrect_fetchall(self):

        cur = self.con.cursor()
        logger.info("Incorrect usage of fetchall")
        cur.execute("create or replace table test (xint int)")
        cur.executemany("select * from test")
        with pytest.raises(Exception) as e:
            cur.fetchall(5)
        cur.close()
        assert "Bad argument to fetchall" in str(e.value)

    def test_incorrect_fetchone(self):

        cur = self.con.cursor()
        logger.info("Incorrect usage of fetchone")
        cur.execute("create or replace table test (xint int)")
        cur.executemany("select * from test")
        with pytest.raises(Exception) as e:
            cur.fetchone(5)
        cur.close()
        assert "Bad argument to fetchone" in str(e.value)

    def test_multi_statement(self):

        cur = self.con.cursor()
        logger.info("Multi statements test")
        with pytest.raises(Exception) as e:
            cur.execute("select 1; select 1;")
        cur.close()
        assert "expected one statement, got" in str(e.value)

    @pytest.mark.skip(reason="Moved to special test package")
    def test_parameterized_query(self):

        cur = self.con.cursor()
        logger.info("Parametered query tests")
        params = 6
        cur.execute("create or replace table test (xint int)")
        cur.executemany('insert into test values (?)', [(5,), (6,), (7,)])
        with pytest.raises(Exception) as e:
            cur.execute('select * from test where xint > ?', str(params))
        cur.close()
        assert "Parametered queries not supported" in str(e.value)

    def test_execute_closed_cursor(self):

        cur = self.con.cursor()
        logger.info("running execute on a closed cursor")
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


class TestCursor(TestBaseWithoutBeforeAfter):

    def test_cursor_through_clustered(self):
        con_clustered = pysqream.connect(self.ip, 3108, 'master', 'sqream', 'sqream', clustered=True)
        cur = con_clustered.cursor()
        assert cur.execute("select 1").fetchall()[0][0] == 1
        cur.close()

    def test_two_statements_same_cursor(self):
        vals = [1]
        con = connect_dbapi(self.ip)
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

    def test_cursor_when_open_statement(self):
        con = connect_dbapi(self.ip)
        cur = con.cursor()
        cur.execute("select 1")
        sleep(10)
        cur.execute("select 1")
        res = cur.fetchall()[0][0]
        cur.close()
        con.close()
        assert res == 1

    def test_fetch_after_all_read(self):
        con = connect_dbapi(self.ip)
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