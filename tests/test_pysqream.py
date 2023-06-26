"""Tests for whole pytest package except the arrays

TODO: Separate by modules
"""
import threading
from datetime import datetime, timezone
from queue import Queue

import pytest
import pysqream
from pysqream import casting

from .base import TestBase, Logger, connect_dbapi


q = Queue()


class TestConnection:

    def test_connection(self, ip_address):

        Logger().info("Connection tests - wrong ip")
        try:
            pysqream.connect('123.4.5.6', 5000, 'master', 'sqream', 'sqream', False, False)
        except Exception as e:
            if "perhaps wrong IP?" not in repr(e):
                raise Exception("bad error message")

        Logger().info("Connection tests - wrong port")
        try:
            pysqream.connect(ip_address, 6000, 'master', 'sqream', 'sqream', False, False)
        except Exception as e:
            if "Connection refused" not in repr(e):
                raise Exception("bad error message")

        Logger().info("Connection tests - wrong database")
        try:
            pysqream.connect(ip_address, 5000, 'wrong_db', 'sqream', 'sqream', False, False)
        except Exception as e:
            if "Database 'wrong_db' does not exist" not in repr(e):
                raise Exception("bad error message")

        Logger().info("Connection tests - wrong username")
        try:
            pysqream.connect(ip_address, 5000, 'master', 'wrong_username', 'sqream', False, False)
        except Exception as e:
            if "role 'wrong_username' doesn't exist" not in repr(e):
                raise Exception("bad error message")

        Logger().info("Connection tests - wrong password")
        try:
            pysqream.connect(ip_address, 5000, 'master', 'sqream', 'wrong_pw', False, False)
        except Exception as e:
            if "wrong password for role 'sqream'" not in repr(e):
                raise Exception("bad error message")

        Logger().info("Connection tests - close() function")
        con = connect_dbapi(ip_address)
        cur = con.cursor()
        con.close()
        try:
            cur.execute('select 1')
        except Exception as e:
          if "Connection has been closed" not in repr(e):
              raise Exception("bad error message")

        Logger().info("Connection tests - close_connection() function")
        con = connect_dbapi(ip_address)
        cur = con.cursor()
        con.close()
        try:
            cur.execute('select 1')
        except Exception as e:
            if "Connection has been closed" not in repr(e):
                raise Exception("bad error message")

        Logger().info("Connection tests - Trying to close a connection that is already closed with close()")
        con = connect_dbapi(ip_address)
        con.close()
        try:
            con.close()
        except Exception as e:
            if "Trying to close a connection that's already closed" not in repr(e):
                raise Exception("bad error message")
        #
        Logger().info("Connection tests - Trying to close a connection that is already closed with close_connection()")
        con = connect_dbapi(ip_address)
        con.close()
        try:
            con.close()
        except Exception as e:
            if "Trying to close a connection that's already closed" not in repr(e):
                raise Exception("bad error message")
        #
        # Logger().info("Connection tests - negative test for use_ssl=True")
        # try:
        #     pysqream.connect(ip_address, 5000, 'master', 'sqream', 'sqream', False, True)
        # except Exception as e:
        #     if "Using use_ssl=True but connected to non ssl sqreamd port" not in repr(e):
        #         raise Exception("bad error message")

        # Logger().info("Connection tests - positive test for use_ssl=True")
        # con = connect_dbapi(ip_address, False, True)
        # cur = con.cursor()
        # res = cur.execute('select 1').fetchall()[0][0]
        # if res != 1:
        #     if f'expected to get 1, instead got {res}' not in repr(e):
        #         raise Exception("bad error message")

        Logger().info("Connection tests - negative test for clustered=True")
        try:
            pysqream.connect(ip_address, 5000, 'master', 'sqream', 'sqream', True, False)
        except Exception as e:
            if "Connected with clustered=True, but apparently not a server picker port" not in repr(e):
                raise Exception("bad error message")

        Logger().info("Connection tests - positive test for clustered=True")
        con = pysqream.connect(ip_address, 3108, "master", "sqream", "sqream", clustered=True)
        cur = con.cursor()
        cur.execute('select 1')
        res = cur.fetchall()[0][0]
        if res != 1:
            if f'expected to get 1, instead got {res}' not in repr(e):
                raise Exception("bad error message")
        con.close()

        Logger().info("Connection tests - both clustered and use_ssl flags on True")
        con = connect_dbapi(ip_address, True, True)
        cur = con.cursor()
        res = cur.execute('select 1').fetchall()[0][0]
        if res != 1:
            if f'expected to get 1, instead got {res}' not in repr(e):
                raise Exception("bad error message")
        con.close()

        Logger().info("Connection tests - all connections are closed when trying to close a single connection SQ-12821")
        con1 = connect_dbapi(ip_address, True, True)
        con2 = connect_dbapi(ip_address, True, True)
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



class TestDatetime(TestBase):

    def test_datetime(self):

        cur = self.con.cursor()
        Logger().info("Datetime tests - insert different timezones datetime")
        t1 = datetime.strptime(datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M"), '%Y-%m-%d %H:%M')
        t2 = datetime.strptime(datetime.now().strftime("%Y-%m-%d %H:%M"), '%Y-%m-%d %H:%M')
        cur.execute("create or replace table test (xdatetime datetime)")
        cur.executemany('insert into test values (?)', [(t1,), (t2,)])
        cur.execute("select * from test")
        res = cur.fetchall()
        if res[0][0] == res[1][0]:
            raise Exception("expected to get different datetimes")

        Logger().info("Datetime tests - insert datetime with microseconds")
        t1 = datetime(1997, 5, 9, 4, 30, 10, 123456)
        t2 = datetime(1997, 5, 9, 4, 30, 10, 987654)
        cur.execute("create or replace table test (xdatetime datetime)")
        cur.executemany('insert into test values (?)', [(t1,), (t2,)])

        cur.close()


class TestThreads:

    def connect_and_execute(self, num, con):
        cur = con.cursor()
        cur.execute("select {}".format(num))
        res = cur.fetchall()
        q.put(res)
        cur.close()

    def test_threads(self, ip_address):
        con = connect_dbapi(ip_address)

        Logger().info("Thread tests - concurrent inserts with multiple threads through cursor")
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


class TestDatetimeUnitTest:

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


# def copy_tests():
#     global con
#     cur = con.cursor()
#     print("loading a csv file into a table through dbapi")
#     cur.execute("create or replace table t (xint1 int, xint2 int, xbigint1 bigint, xbigint2 bigint, xdouble1 double,"
#                 "xint3 int,xdouble2 double, xdate date, xdatetime datetime,xint4 int, xtext text, xint5 int, xint6 int)")
#     cur.csv_to_table(os.path.join(os.path.abspath("."), "t.csv"), "t", delimiter="|")
#     cur.execute("select count(*) from t")
#     res = cur.fetchall()[0][0]
#     if res != 2000:
#         raise Exception("expected to get 2000, instead got {}".format(res))
