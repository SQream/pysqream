from datetime import datetime, date, timezone
from numpy.random import randint, uniform, random, choice
from numpy import round
from queue import Queue
from time import sleep

import pandas as pd
import pysqream
import sys
import threading
from pysqream import casting
from faker import Faker

from tests.test_base import TestBase, TestBaseWithoutBeforeAfter, Logger, connect_dbapi


q = Queue()
varchar_length = 10
nvarchar_length = 10
max_bigint = sys.maxsize if sys.platform not in ('win32', 'cygwin') else 2147483647
logger = Logger()


def generate_varchar(length):
    return ''.join(chr(num) for num in randint(32, 128, length))


col_types = {'bool', 'tinyint', 'smallint', 'int', 'bigint', 'real', 'double', 'date', 'datetime',
             'varchar({})'.format(varchar_length), 'nvarchar({})'.format(varchar_length)}


pos_test_vals = {'bool': (0, 1, True, False, 2, 3.6, 'test', (1997, 5, 9), (1997, 12, 12, 10, 10, 10)),
                 'tinyint': (randint(0, 255), randint(0, 255), 0, 255, True, False),
                 'smallint': (randint(-32768, 32767), 0, -32768, 32767, True, False),
                 'int': (randint(-2147483648, 2147483647), 0, -2147483648, 2147483647, True, False),
                 'bigint': (randint(1-max_bigint, max_bigint), 0, 1-max_bigint, max_bigint, True, False),
                 'real': (float('inf'), float('-inf'), float('+0'), float('-0'), round(uniform(1e-6, 1e6), 5), 837326.52428, True, False),   # float('nan')
                 'double': (float('inf'), float('-inf'), float('+0'), float('-0'), uniform(1e-6, 1e6), True, False),  # float('nan')
                 'date': (date(1998, 9, 24), date(2020, 12, 1), date(1997, 5, 9), date(1993, 7, 13)),
                 'datetime': (datetime(1001, 1, 1, 10, 10, 10), datetime(1997, 11, 30, 10, 10, 10), datetime(1987, 7, 27, 20, 15, 45), datetime(1993, 12, 20, 17, 25, 46)),
                 'varchar': (generate_varchar(varchar_length), generate_varchar(varchar_length), generate_varchar(varchar_length), 'b   '),
                 'nvarchar': ('א', 'א  ', '', 'ab א')}

neg_test_vals = {'tinyint': (258, 3.6, 'test',  (1997, 5, 9), (1997, 12, 12, 10, 10, 10)),
                 'smallint': (40000, 3.6, 'test', (1997, 5, 9), (1997, 12, 12, 10, 10, 10)),
                 'int': (9999999999, 3.6, 'test',  (1997, 5, 9), (1997, 12, 12, 10, 10, 10)),
                 'bigint': (92233720368547758070, 3.6, 'test', (1997, 12, 12, 10, 10, 10)),
                 'real': ('test', (1997, 12, 12, 10, 10, 10)),
                 'double': ('test', (1997, 12, 12, 10, 10, 10)),
                 'date': (5, 3.6, (-8, 9, 1), (2012, 15, 6), (2012, 9, 45), 'test', False, True),
                 'datetime': (5, 3.6, (-8, 9, 1, 0, 0, 0), (2012, 15, 6, 0, 0, 0), (2012, 9, 45, 0, 0, 0), (2012, 9, 14, 26, 0, 0), (2012, 9, 14, 13, 89, 0), 'test', False, True),
                 'varchar': (5, 3.6, (1, 2), (1997, 12, 12, 10, 10, 10), False, True),
                 'nvarchar': (5, 3.6, (1, 2), (1997, 12, 12, 10, 10, 10), False, True)}


class TestConnection(TestBaseWithoutBeforeAfter):

    def test_wrong_ip(self):
        logger.info("Connection tests - wrong ip")
        try:
            pysqream.connect('123.4.5.6', 5000, 'master', 'sqream', 'sqream', False, False)
        except Exception as e:
            if "perhaps wrong IP?" not in repr(e):
                raise Exception("bad error message")

    def test_wrong_port(self):
        logger.info("Connection tests - wrong port")
        try:
            pysqream.connect(self.ip, 6000, 'master', 'sqream', 'sqream', False, False)
        except Exception as e:
            if "Connection refused" not in repr(e):
                raise Exception("bad error message")

    def test_wrong_database(self):
        logger.info("Connection tests - wrong database")
        try:
            pysqream.connect(self.ip, 5000, 'wrong_db', 'sqream', 'sqream', False, False)
        except Exception as e:
            if "Database wrong_db no longer exists" not in repr(e):
                raise Exception("bad error message")

    def test_wrong_username(self):
        logger.info("Connection tests - wrong username")
        try:
            pysqream.connect(self.ip, 5000, 'master', 'wrong_username', 'sqream', False, False)
        except Exception as e:
            if "role 'wrong_username' doesn't exist" not in repr(e):
                raise Exception("bad error message")

    def test_wrong_password(self):
        logger.info("Connection tests - wrong password")
        try:
            pysqream.connect(self.ip, 5000, 'master', 'sqream', 'wrong_pw', False, False)
        except Exception as e:
            if "wrong password for role 'sqream'" not in repr(e):
                raise Exception("bad error message")

    def test_close_connection(self):
        logger.info("Connection tests - close_connection() function")
        con = connect_dbapi(self.ip, clustered=self.clustered, use_ssl=self.use_ssl, port=self.port,
                            picker_port=self.picker_port, database=self.database,
                            username=self.username, password=self.password)
        cur = con.cursor()
        con.close()
        try:
            cur.execute('select 1')
        except Exception as e:
            if "Connection has been closed" not in repr(e):
                raise Exception("bad error message")

        logger.info("Connection tests - Trying to close a connection that is already closed with close()")
        con = connect_dbapi(self.ip, clustered=self.clustered, use_ssl=self.use_ssl, port=self.port,
                            picker_port=self.picker_port, database=self.database,
                            username=self.username, password=self.password)
        con.close()
        try:
            con.close()
        except Exception as e:
            if "Trying to close a connection that's already closed" not in repr(e):
                raise Exception("bad error message")

    def test_negative_clustered(self):
        logger.info("Connection tests - negative test for clustered=True")
        try:
            pysqream.connect(self.ip, 5000, 'master', 'sqream', 'sqream', True, False)
        except Exception as e:
            if "Connected with clustered=True, but apparently not a server picker port" not in repr(e):
                raise Exception("bad error message")

    def test_positive_clustered(self):
        logger.info("Connection tests - positive test for clustered=True")
        con = pysqream.connect(self.ip, self.picker_port, "master", "sqream", "sqream", clustered=True)
        cur = con.cursor()
        cur.execute('select 1')
        res = cur.fetchall()[0][0]
        if res != 1:
            if f'expected to get 1, instead got {res}' not in repr(e):
                raise Exception("bad error message")
        con.close()

        logger.info("Connection tests - both clustered and use_ssl flags on True")
        con = connect_dbapi(self.ip, clustered=self.clustered, use_ssl=self.use_ssl, port=self.port,
                            picker_port=self.picker_port, database=self.database,
                            username=self.username, password=self.password)
        cur = con.cursor()
        res = cur.execute('select 1').fetchall()[0][0]
        if res != 1:
            if f'expected to get 1, instead got {res}' not in repr(e):
                raise Exception("bad error message")
        con.close()

    def test_sq_12821(self):
        logger.info("Connection tests - all connections are closed when trying to close a single connection SQ-12821")
        con1 = connect_dbapi(self.ip, clustered=self.clustered, use_ssl=self.use_ssl, port=self.port,
                             picker_port=self.picker_port, database=self.database,
                             username=self.username, password=self.password)
        con2 = connect_dbapi(self.ip, clustered=self.clustered, use_ssl=self.use_ssl, port=self.port,
                             picker_port=self.picker_port, database=self.database,
                             username=self.username, password=self.password)
        cur = con1.cursor()
        cur.execute('select 1')
        cur.fetchall()
        cur.close()
        con2.close()
        cur = con1.cursor()
        cur.execute('select 1')
        cur.fetchall()
        cur.close()
        con1.close()


class TestPositive(TestBase):

    def test_positive(self):
        cur = self.con.cursor()
        for col_type in col_types:
            trimmed_col_type = col_type.split('(')[0]

            logger.info(f'Positive tests - Inserted values test for column type {col_type}')
            cur.execute(f"create or replace table test (t_{trimmed_col_type} {col_type})")
            for val in pos_test_vals[trimmed_col_type]:
                cur.execute('truncate table test')
                rows = [(val,)]
                cur.executemany("insert into test values (?)", rows)
                res = cur.execute("select * from test").fetchall()[0][0]

                # Compare
                if val != res:
                    if trimmed_col_type not in ('bool', 'varchar', 'date', 'datetime', 'real'):
                        logger.info((repr(val), type(val), repr(res), type(res)))
                        raise Exception("TEST ERROR: No match between the expected result to the returned result. expected to get {}, instead got {} on datatype {}".format(repr(val), repr(res), trimmed_col_type))
                    elif trimmed_col_type == 'bool' and val != 0:
                        if res is not True:
                            raise Exception("TEST ERROR: No match between the expected result to the returned result. expected to get 'True', instead got {} on datatype {}".format(repr(res), trimmed_col_type))
                    elif trimmed_col_type == 'varchar' and val.strip() != res:
                        raise Exception("TEST ERROR: No match between the expected result to the returned result. expected to get {}, instead got {} on datatype {}".format(repr(val), repr(res), trimmed_col_type))
                    elif trimmed_col_type in ('date', 'datetime') and datetime(*val) != res and date(*val) != res:
                        raise Exception("TEST ERROR: No match between the expected result to the returned result. expected to get {}, instead got {} on datatype {}".format(repr(val), repr(res), trimmed_col_type))
                    elif trimmed_col_type == 'real' and abs(res-val) > 0.1:
                        # Single precision packing and unpacking is inaccurate:
                        # unpack('f', pack('f', 255759.83335))[0] == 255759.828125
                        raise Exception("TEST ERROR: No match between the expected result to the returned result. expected to get {}, instead got {} on datatype {}".format(repr(val), repr(res), trimmed_col_type))

            logger.info(f'Positive tests - Null test for column type: {col_type}')
            cur.execute("create or replace table test (t_{} {})".format(trimmed_col_type, col_type))
            cur.executemany('insert into test values (?)', [(None,)])
            res = cur.execute('select * from test').fetchall()[0][0]
            if res not in (None,):
                raise Exception("TEST ERROR: Error setting null on column type: {}\nGot: {}, {}".format(trimmed_col_type, res, type(res)))

        logger.info("Positive tests - Case statement with nulls")
        cur.execute("create or replace table test (xint int)")
        cur.executemany('insert into test values (?)', [(5,), (None,), (6,), (7,), (None,), (8,), (None,)])
        cur.executemany("select case when xint is null then 1 else 0 end from test")
        expected_list = [0, 1, 0, 0, 1, 0, 1]
        res_list = []
        res_list += [x[0] for x in cur.fetchall()]
        if expected_list != res_list:
            raise Exception("expected to get {}, instead got {}".format(expected_list, res_list))

        logger.info("Positive tests - Testing select true/false")
        cur.execute("select false")
        res = cur.fetchall()[0][0]
        if res != 0:
            raise Exception("Expected to get result 0, instead got {}".format(res))
        cur.execute("select true")
        res = cur.fetchall()[0][0]
        if res != 1:
            raise Exception("Expected to get result 1, instead got {}".format(res))

        logger.info("Positive tests - Running a statement when there is an open statement")
        cur.execute("select 1")
        sleep(10)
        res = cur.execute("select 1").fetchall()[0][0]
        if res != 1:
            raise Exception(f'expected to get result 1, instead got {res}')

        cur.close()


class TestNegative(TestBase):

    def test_negative(self):
        """Negative Set/Get tests"""

        cur = self.con.cursor()

        for col_type in col_types:
            if col_type == 'bool':
                continue
            logger.info("Negative tests for column type: {}".format(col_type))
            trimmed_col_type = col_type.split('(')[0]
            logger.info("prepare a table")
            cur.execute("create or replace table test (t_{} {})".format(trimmed_col_type, col_type))
            for val in neg_test_vals[trimmed_col_type]:
                logger.info("Insert value {} into data type {}".format(repr(val), repr(trimmed_col_type)))
                rows = [(val,)]
                try:
                    cur.executemany("insert into test values (?)", rows)
                except Exception as e:
                    if "Error packing columns. Check that all types match the respective column types" not in repr(e):
                        raise Exception('bad error message')

        logger.info("Negative tests - Inconsistent sizes test")
        cur.execute("create or replace table test (xint int, yint int)")
        try:
            cur.executemany('insert into test values (?, ?)', [(5,), (6, 9), (7, 8)])
        except Exception as e:
            if "Incosistent data sequences passed for inserting. Please use rows/columns of consistent length" not in repr(e):
                raise Exception('bad error message')

        logger.info("Negative tests - Varchar - Conversion of a varchar to a smaller length")
        cur.execute("create or replace table test (test varchar(10))")
        try:
            cur.executemany("insert into test values ('aa12345678910')")
        except Exception as e:
            if "expected response statementPrepared but got" not in repr(e):
                            raise Exception('bad error message')

        logger.info("Negative tests - Nvarchar - Conversion of a varchar to a smaller length")
        cur.execute("create or replace table test (test nvarchar(10))")
        try:
            cur.executemany("insert into test values ('aa12345678910')")
        except Exception as e:
            if "expected response executed but got" not in repr(e):
                raise Exception('bad error message')

        logger.info("Negative tests - Incorrect usage of fetchmany - fetch without a statement")
        cur.execute("create or replace table test (xint int)")
        try:
            cur.fetchmany(2)
        except Exception as e:
            if "No open statement while attempting fetch operation" not in repr(e):
                raise Exception('bad error message')

        logger.info("Negative tests - Incorrect usage of fetchall")
        cur.execute("create or replace table test (xint int)")
        cur.executemany("select * from test")
        try:
            cur.fetchall(5)
        except Exception as e:
            if "Bad argument to fetchall" not in repr(e):
                raise Exception('bad error message')

        logger.info("Negative tests - Incorrect usage of fetchone")
        cur.execute("create or replace table test (xint int)")
        cur.executemany("select * from test")
        try:
            cur.fetchone(5)
        except Exception as e:
            if "Bad argument to fetchone" not in repr(e):
                raise Exception('bad error message')

        logger.info("Negative tests - Multi statements test")
        try:
            cur.execute("select 1; select 1;")
        except Exception as e:
            if "expected one statement, got " not in repr(e):
                raise Exception('bad error message')

        logger.info("Negative tests - Parametered query tests")
        params = 6
        cur.execute("create or replace table test (xint int)")
        cur.executemany('insert into test values (?)', [(5,), (6,), (7,)])
        try:
            cur.execute('select * from test where xint > ?', str(params))
        except Exception as e:
            if "Error packing columns. Check that all types match the respective column types" not in repr(e):
                raise Exception('bad error message')

        logger.info("Negative tests - running execute on a closed cursor")
        cur.close()
        cur = self.con.cursor()
        cur.close()
        try:
            cur.execute("select 1")
        except Exception as e:
            if "Cursor has been closed" not in repr(e):
                raise Exception('bad error message')


class TestFetch(TestBase):

    def test_fetch(self):
        cur = self.con.cursor()

        logger.info("Fetch tests - positive fetch tests")
        cur.execute("create or replace table test (xint int)")
        cur.executemany('insert into test values (?)', [(1,), (2,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,)])
        # fetchmany(1) vs fetchone()
        cur.execute("select * from test")
        res = cur.fetchmany(1)[0][0]
        cur.execute("select * from test")
        res2 = cur.fetchone()[0]
        if res != res2:
            raise Exception(f"fetchmany(1) and fetchone() didn't return the same value. fetchmany(1) is {res} and fetchone() is {res2}")
        # fetchmany(-1) vs fetchall()
        cur.execute("select * from test")
        res3 = cur.fetchmany(-1)
        cur.execute("select * from test")
        res4 = cur.fetchall()
        if res3 != res4:
            raise Exception("fetchmany(-1) and fetchall() didn't return the same value. fetchmany(-1) is {} and fetchall() is {}".format(res3, res4))
        # fetchone() loop
        cur.execute("select * from test")
        for i in range(1, 11):
            x = cur.fetchone()[0]
            if x != i:
                raise Exception("fetchone() returned {} instead of {}".format(x, i))

        logger.info("Fetch tests - combined fetch functions")
        cur.execute("create or replace table test (xint int)")
        cur.executemany('insert into test values (?)', [(1,), (2,), (3,), (4,), (5,), (6,), (7,), (8,), (9,), (10,)])
        cur.execute("select * from test")
        expected_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        res_list = []
        res_list.append(cur.fetchone()[0])
        res_list += [x[0] for x in cur.fetchmany(2)]
        res_list.append(cur.fetchone()[0])
        res_list += [x[0] for x in cur.fetchall()]
        if expected_list != res_list:
            raise Exception("expected to get {}, instead got {}".format(expected_list, res_list))

        logger.info("Fetch tests - fetch functions after all the data has already been read")
        cur.execute("create or replace table test (xint int)")
        cur.executemany('insert into test values (?)', [(1,)])
        cur.execute("select * from test")
        x = cur.fetchone()[0]
        res = cur.fetchone()
        if res is not None:
            raise Exception(f"expected to get an empty result from fetchone, instead got {res}")
        res = cur.fetchall()
        if res != []:
            raise Exception(f"expected to get an empty result from fetchall, instead got {res}")
        res = cur.fetchmany(1)
        if res != []:
            raise Exception(f"expected to get an empty result from fetchmany, instead got {res}")

        cur.close()


class TestCursor(TestBaseWithoutBeforeAfter):

    def test_cursor(self):
        logger.info("Cursor tests - running two statements on the same cursor connection")
        vals = [1]
        con = connect_dbapi(self.ip, clustered=self.clustered, use_ssl=self.use_ssl, port=self.port,
                            picker_port=self.picker_port, database=self.database,
                            username=self.username, password=self.password)
        cur = con.cursor()
        cur.execute("select 1")
        res1 = cur.fetchall()[0][0]
        vals.append(res1)
        cur.execute("select 1")
        res2 = cur.fetchall()[0][0]
        vals.append(res2)
        if not all(x == vals[0] for x in vals):
            raise Exception(f"expected to get result 1, instead got {res1} and {res2}")
        cur.close()
        con.close()

        con = connect_dbapi(self.ip, clustered=self.clustered, use_ssl=self.use_ssl, port=self.port,
                            picker_port=self.picker_port, database=self.database,
                            username=self.username, password=self.password)
        logger.info("Cursor tests - running a statement through cursor when there is an open statement")
        cur = con.cursor()
        cur.execute("select 1")
        sleep(10)
        cur.execute("select 1")
        res = cur.fetchall()[0][0]
        if res != 1:
            raise Exception(f"expected to get result 1, instead got {res}")
        cur.close()
        con.close()

        logger.info("Cursor tests - fetch functions after all the data has already been read through cursor")
        con = connect_dbapi(self.ip, clustered=self.clustered, use_ssl=self.use_ssl, port=self.port,
                            picker_port=self.picker_port, database=self.database,
                            username=self.username, password=self.password)
        cur = con.cursor()
        cur.execute("create or replace table test (xint int)")
        cur.executemany('insert into test values (?)', [(1,)])
        cur.execute("select * from test")
        x = cur.fetchone()[0]
        res = cur.fetchone()
        if res is not None:
            raise Exception("expected to get an empty result from fetchone, instead got {}".format(res))
        res = cur.fetchall()
        if res != []:
            raise Exception("expected to get an empty result from fetchall, instead got {}".format(res))
        res = cur.fetchmany(1)
        if res != []:
            raise Exception("expected to get an empty result from fetchmany, instead got {}".format(res))
        cur.close()
        con.close()

        logger.info("Cursor tests - run a query through a cursor and close the connection directly")
        con = connect_dbapi(self.ip, clustered=self.clustered, use_ssl=self.use_ssl, port=self.port,
                            picker_port=self.picker_port, database=self.database,
                            username=self.username, password=self.password)
        cur = con.cursor()
        cur.execute("select 1")
        con.close()
        if not cur.closed:
            raise Exception('Closed a connection after running a query through a cursor, but cursor is still open')


class TestString(TestBase):

    def test_string(self):

        cur = self.con.cursor()
        logger.info("String tests - insert and return UTF-8")
        cur.execute("create or replace table test (xvarchar varchar(20))")
        cur.executemany('insert into test values (?)', [(u"hello world",), ("hello world",)])
        cur.execute("select * from test")
        res = cur.fetchall()
        if res[0][0] != res[1][0]:
            raise Exception("expected to get identical strings from select statement. instead got {} and {}".format(res[0][0], res[1][0]))

        logger.info("String tests - strings with escaped characters")
        cur.execute("create or replace table test (xvarchar varchar(20))")
        values = [("\t",), ("\n",), ("\\n",), ("\\\n",), (" \\",), ("\\\\",), (" \nt",), ("'abd''ef'",), ("abd""ef",), ("abd\"ef",)]
        cur.executemany('insert into test values (?)', values)
        cur.executemany("select * from test")
        expected_list = ['', '', '\\n', '\\', ' \\', '\\\\', ' \nt', "'abd''ef'", 'abdef', 'abd"ef']
        res_list = []
        res_list += [x[0] for x in cur.fetchall()]
        if expected_list != res_list:
            raise Exception("expected to get {}, instead got {}".format(expected_list, res_list))

        cur.close()


class TestDatetime(TestBase):

    def test_datetime(self):

        cur = self.con.cursor()
        logger.info("Datetime tests - insert different timezones datetime")
        t1 = datetime.strptime(datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M"), '%Y-%m-%d %H:%M')
        t2 = datetime.strptime(datetime.now().strftime("%Y-%m-%d %H:%M"), '%Y-%m-%d %H:%M')
        cur.execute("create or replace table test (xdatetime datetime)")
        cur.executemany('insert into test values (?)', [(t1,), (t2,)])
        cur.execute("select * from test")
        res = cur.fetchall()
        if res[0][0] == res[1][0]:
            raise Exception("expected to get different datetimes")

        logger.info("Datetime tests - insert datetime with microseconds")
        t1 = datetime(1997, 5, 9, 4, 30, 10, 123456)
        t2 = datetime(1997, 5, 9, 4, 30, 10, 987654)
        cur.execute("create or replace table test (xdatetime datetime)")
        cur.executemany('insert into test values (?)', [(t1,), (t2,)])

        cur.close()


class TestThreads(TestBaseWithoutBeforeAfter):

    def connect_and_execute(self, num, con):
        cur = con.cursor()
        cur.execute("select {}".format(num))
        res = cur.fetchall()
        q.put(res)
        cur.close()

    def test_threads(self):
        con = connect_dbapi(self.ip, clustered=self.clustered, use_ssl=self.use_ssl, port=self.port,
                            picker_port=self.picker_port, database=self.database,
                            username=self.username, password=self.password)

        logger.info("Thread tests - concurrent inserts with multiple threads through cursor")
        t1 = threading.Thread(target=self.connect_and_execute, args=(3, con,))
        t2 = threading.Thread(target=self.connect_and_execute, args=(3, con,))
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        res1 = q.get()[0][0]
        res2 = q.get()[0][0]
        if res1 != res2:
            raise Exception("expected to get equal values. instead got res1 {} and res2 {}".format(res1, res2))

        con.close()


class TestDatetimeUnitTest(TestBaseWithoutBeforeAfter):

    def test_zero_date(self):
        error = 'year 0 is out of range'
        try:
            casting.sq_date_to_py_date(0, is_null=False)
        except Exception as e:
            if error not in str(e):
                raise ValueError(f"Excepted to get error [{error}], got [{str(e)}]")

    def test_zero_datetime(self):
        error = 'year 0 is out of range'
        try:
            casting.sq_datetime_to_py_datetime(0, is_null=False)
        except Exception as e:
            if error not in str(e):
                raise ValueError(f"Excepted to get error [{error}], got [{str(e)}]")

    def test_negative_date(self):
        error = 'year -9 is out of range'
        try:
            casting.sq_date_to_py_date(-3000, is_null=False)
        except Exception as e:
            if error not in str(e):
                raise ValueError(f"Excepted to get error [{error}], got [{str(e)}]")

    def test_negative_datetime(self):
        error = 'year -9 is out of range'
        try:
            casting.sq_datetime_to_py_datetime(-3000, is_null=False)
        except Exception as e:
            if error in str(e):
                raise ValueError(f"Excepted to get error [{error}], got [{str(e)}]")

    def test_null_date(self):
        res = casting.sq_date_to_py_date(-3000, is_null=True)
        if res is not None:
            raise ValueError(f"Excepted to get None, but got [{res}]")

    def test_null_datetime(self):
        res = casting.sq_datetime_to_py_datetime(-3000, is_null=True)
        if res is not None:
            raise ValueError(f"Excepted to get None, but got [{res}]")


class TestBigData(TestBase):

    def load_ddl(self, ddl_file_path):
        logger.info("Load DDL")
        cur = self.con.cursor()
        with open(ddl_file_path, 'r') as file:
            ddl_string = file.read()
        cur.execute(ddl_string)
        cur.close()

    def generate_data(self):
        logger.info("Generate Data")
        fake = Faker()
        data = {
            "column1": randint(-2147483648, 2147483647, self.num_rows),
            "column2": randint(-9223372036854775808, 9223372036854775807, self.num_rows),
            "column3": randint(-32768, 32767, self.num_rows),
            "column4": [round(random(), 38) for _ in range(self.num_rows)],
            "column5": [round(random() * 10 ** 10, 10) for _ in range(self.num_rows)],
            "column6": [fake.date_time_this_decade() for _ in range(self.num_rows)],
            "column7": [fake.date_this_decade() for _ in range(self.num_rows)],
            "column8": [choice([True, False]) for _ in range(self.num_rows)],
            "column9": randint(0, 256, self.num_rows),
            "column10": uniform(-1e5, 1e5, self.num_rows),
            "column11": uniform(-3.4e38, 3.4e38, self.num_rows)
        }
        for i in range(12, 101):
            data[f"column{i}"] = [fake.text(max_nb_chars=100) for _ in range(self.num_rows)]
        df = pd.DataFrame(data)
        duplicated_df = df.loc[df.index.repeat(self.repeat)].reset_index(drop=True)
        return list(duplicated_df.itertuples(index=False, name=None))

    def start_execute_network_insert(self):
        logger.info("Start to execute network insert")
        num_values = ", ".join(["?" for i in range(100)])
        insert_query = f"insert into big_data values ({num_values})"
        cur = self.con.cursor()
        cur.executemany(insert_query, self.data)
        cur.close()

    def compare_results(self):
        logger.info("Compare results")
        cur = self.con.cursor()
        cur.execute("SELECT COUNT(*) FROM big_data")
        res = cur.fetchall()[0][0]
        if res != self.expected_rows:
            raise Exception(f"Expected to get result {self.expected_rows}, instead got {res}")

    def test_network_insert_big_data_sq_18040(self, big_data_ddl_path):
        self.repeat = 1500
        self.num_rows = 1000
        self.expected_rows = self.num_rows * self.repeat
        self.load_ddl(ddl_file_path=big_data_ddl_path)
        self.data = self.generate_data()
        self.start_execute_network_insert()
        self.compare_results()



