"""Tests for whole pytest package except the arrays

TODO: Separate by modules
"""
import ssl
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from socket import socket

import pytest
from pysqream import connect
from pysqream.casting import sq_date_to_py_date, sq_datetime_to_py_datetime
from pysqream.connection import Connection
from pysqream.SQSocket import SQSocket, Client
from pysqream.utils import ProgrammingError, NonSSLPortError

from .utils import ensure_empty_table, select

TEMP_TABLE = "pysqream_test_pysqream_temp"


class BaseTestConnection:
    """
    Base for connection tests, that provide fixture and connection factory
    """

    @pytest.fixture
    def patch_response(self, monkeypatch):
        """Utility to mock _open_connection and server response"""
        def _m_open_connection(obj, *_):
            obj.connect_to_socket = True
            obj.client = Client(None)
            obj.s = socket()
            obj.ip, obj.port = obj.orig_ip, obj.orig_port
        monkeypatch.setattr(Connection, '_open_connection', _m_open_connection)

        def _patch_response(msg='{"connectionId": 123}'):
            monkeypatch.setattr(Client, 'send_string', lambda *_: msg)
        yield _patch_response

    def connect(self, host, port, dbname='master', username='sqream',
                password='sqream', *args, **kwargs):
        """Wrapper for connection factory that provide default values"""
        # to conform pysqream.connect function
        # pylint: disable=keyword-arg-before-vararg,too-many-arguments
        __tracebackhide__ = True  # pylint: disable=unused-variable
        return connect(host, port, dbname, username, password, *args, **kwargs)


# TODO: Part of it should be tests of pysqream.SQSocket.SQSocket / Client
# or pysqream.connection.Connection
@pytest.mark.mock_connection
class TestConnection(BaseTestConnection):
    """
    Test exceptions on connection and connections does not affect each other
    """

    def test_connection_wrong_ip_reraise(self, monkeypatch):
        """
        Test that TimeoutError on connection is reraised  with "wrong IP"
        """
        # Set small timeout for test speed, don't need to wait too long
        def _set_small_timeout(obj, *_):
            obj.s.settimeout(0.5)
        monkeypatch.setattr(SQSocket, 'timeout', _set_small_timeout)

        with pytest.raises(TimeoutError, match="Timeout when connecting to "
                                               "SQream, perhaps wrong IP?"):
            self.connect('1.1.1.1', 5000)

    def test_connection_wrong_port(self):
        """
        Test that ConnectionRefusedError is reraised with "wrong IP"
        """
        with pytest.raises(ConnectionRefusedError, match="Connection refused, "
                                                         "perhaps wrong IP?"):
            self.connect('127.0.0.1', 1)  # Ensure to connect on closed port

    # Errors of wrong db, username & password are controlled, so
    # TODO: Check only base error in response from server and check wrong json
    def test_connection_wrong_database(self, patch_response, ip_address):
        """Connection to wrong DB produces ProgrammingError"""
        patch_response('{"error": "Database \'wrong_db\' does not exist"}')

        with pytest.raises(ProgrammingError, match=r"Database ['\"]\w+['\"] "
                                                   "does not exist"):
            self.connect(ip_address, 5000, 'wrong_db')

    def test_connection_wrong_login(self, patch_response, ip_address):
        """Connection to DB with wrong username produces ProgrammingError"""
        patch_response('{"error": "Login failure: role \'wrong_username\' '
                       "doesn't exist\"}")

        with pytest.raises(ProgrammingError, match=r"role ['\"]\w+['\"] "
                                                   "doesn't exist"):
            self.connect(ip_address, 5000, username='wrong_username')

    def test_connection_wrong_password(self, patch_response, ip_address):
        """Connection to DB with wrong password produces ProgrammingError"""
        patch_response('{"error": "Login failure: wrong password for role '
                       "'sqream'\"}")

        with pytest.raises(ProgrammingError, match="wrong password for role"):
            self.connect(ip_address, 5000, password='wrong_pw')

    def test_closed_cursor_execute_raise(self, mock_cursor):
        """Closed Cursor raises ProgrammingError on attempt to execute"""
        mock_cursor.close()
        with pytest.raises(
                ProgrammingError, match="Cursor has been closed"):
            mock_cursor.execute('select 1')

    def test_closed_connection_execute_raise(self, patch_response):
        """Cursor raises ProgrammingError if base Connection is closed"""
        patch_response()
        conn = self.connect('1.1.1.1', 5000)
        cur = conn.cursor()
        conn.close()
        with pytest.raises(
                ProgrammingError, match="Connection has been closed"):
            cur.execute('select 1')

    def test_connection_close_closed(self, patch_response):
        """
        Closed connection raises ProgrammingError on attempt to close again
        """
        patch_response()
        con = self.connect('1.1.1.1', 5000)
        con.close()
        with pytest.raises(
                ProgrammingError, match="Trying to close a connection that's "
                                        "already closed"):
            con.close()

    def test_connection_ssl_violation_raises(self, monkeypatch, ip_address):
        """
        Violation of SSL protocol raises pysqream.utils.NonSSLPortError
        """
        def _interrupt(*_, **__):
            raise ssl.SSLEOFError(
                8, 'Mock | EOF occurred in violation of protocol (_ssl.c:997)')
        monkeypatch.setattr(socket, 'connect', _interrupt)

        with pytest.raises(NonSSLPortError):
            self.connect(ip_address, 5000, use_ssl=True)

    def test_connection_ssl_wrong_version(self, monkeypatch, ip_address):
        """Wrong SSL version number raises pysqream.utils.NonSSLPortError"""
        def _interrupt(*_, **__):
            raise ssl.SSLError(1, '[SSL: WRONG_VERSION_NUMBER] wrong version '
                                  'number (_ssl.c:997)')
        monkeypatch.setattr(socket, 'connect', _interrupt)

        with pytest.raises(NonSSLPortError):
            self.connect(ip_address, 5000, use_ssl=True)

    def test_connection_clustered_no_picker(self, monkeypatch, ip_address):
        """
        Attempt to connect to clustered server, but picker port isn't present
        """
        def _timeout(*_, **__):
            raise TimeoutError('timed out')
        monkeypatch.setattr(Client, "receive", _timeout)

        with pytest.raises(
                ProgrammingError, match="Connected with clustered=True, but "
                                        "apparently not a server picker port"):
            self.connect(ip_address, 5000, clustered=True)

    def test_close_connection_but_not_others(self, patch_response):
        """
        Test closing connections do not close others at the same ip / port

        Connected issue SQ-12821
        """
        patch_response()
        con1 = self.connect('1.1.1.1', 5000)
        con2 = self.connect('1.1.1.1', 5000)
        assert con1 is not con2
        cur1 = con1.cursor()
        assert cur1
        con2.close()
        cur2 = con1.cursor()
        assert cur2 not in (cur1, None)


class TestConnectionPositive(BaseTestConnection):
    """Integration tests of successful connection with clustered and use_ssl"""

    def test_connection_positive_clustered_true(self, ip_address, cport):
        """Positive integration test with clustered=True"""
        con = self.connect(ip_address, cport, clustered=True)
        cur = con.cursor()
        assert cur.execute('select 1').fetchall()[0][0] == 1

    def test_connection_positive_clustered_ssl_true(self, ip_address, cport):
        """Positive integration test with clustered=True & use_ssl=True"""
        con = self.connect(ip_address, cport + 1, clustered=True, use_ssl=True)
        cur = con.cursor()
        assert cur.execute('select 1').fetchall()[0][0] == 1


# Integration tests for datetime column
def test_insert_different_timezones(cursor):
    """Tests insertions of the same datetime with different timezones"""
    # TODO: Test different timezones in converters
    # however it really tests nothing but merely two datetimes

    def _round(dtm):
        """
        Rounds datetime microseconds for comparison

        SQream DB stores datetime rounding to milliseconds,
        so python's datetime should be rounded appropriately before
        comparing. Also there is a bug, that pysqream use floor division
        on converting formats https://sqream.atlassian.net/browse/SQ-13969
        """
        # so use floor division for appropriate comparison here too
        return dtm.replace(microsecond=dtm.microsecond // 1000 * 1000)

    cur = cursor
    dt1 = datetime.now()
    dt2 = dt1.astimezone(timezone.utc)
    assert dt1 != dt2
    data = [(dt1,), (dt2,)]
    ensure_empty_table(cursor, TEMP_TABLE, "xdatetime datetime")
    cur.executemany(f'insert into {TEMP_TABLE} values (?)', data)
    results = select(cursor, TEMP_TABLE)
    assert results != data  # Results do not include timezone info
    assert len(results) == 2
    assert results[0][0] == _round(dt1)
    assert results[1][0] != _round(dt2)  # Doesn't include TZ info
    assert results[1][0] == _round(dt2).replace(tzinfo=None)

# test_datetime_mic tested inside test_insert_different_timezones
# because now always includes microseconds


class TestThreads:
    """Tests for fetch operation in threads"""

    def connect_and_execute(self, num, con):
        """Utility to run in separate threads"""
        cur = con.cursor()
        cur.execute(f"select {num}")
        res = cur.fetchall()
        cur.close()
        return res[0][0]

    def test_threads(self, conn):
        """Separate threads would get results"""
        results = []
        with ThreadPoolExecutor() as executor:
            futures = {executor.submit(self.connect_and_execute, i, conn)
                       for i in range(6)}
            results = set()
            for future in as_completed(futures):
                results.add(future.result())
        assert results == {0, 1, 2, 3, 4, 5}


class TestDatetimeUnitTest:
    """
    Unit tests for converters of SQream date & datetime to python datatype
    """
    functions = [sq_date_to_py_date, sq_datetime_to_py_datetime]

    @pytest.mark.parametrize("value", [0, -3000])
    @pytest.mark.parametrize("func", functions)
    def test_value_is_out_of_range(self, func, value):
        """value less then or equal to 0 produce ValueError on date"""
        with pytest.raises(ValueError, match=r"year -?\d+ is out of range"):
            func(value, is_null=False)

    @pytest.mark.parametrize("value", [0, -3000, 3000])
    @pytest.mark.parametrize("func", functions)
    def test_null_date(self, value, func):
        """Result is None if is_null"""
        assert func(value, is_null=True) is None


# Keep for reason of returning
# def copy_tests():
#     global con
#     cur = con.cursor()
#     print("loading a csv file into a table through dbapi")
#     cur.execute("create or replace table t (xint1 int, xint2 int, "
#                 "xbigint1 bigint, xbigint2 bigint, xdouble1 double,"
#                 "xint3 int,xdouble2 double, xdate date, xdatetime datetime,"
#                 "xint4 int, xtext text, xint5 int, xint6 int)")
#     cur.csv_to_table(
#         os.path.join(os.path.abspath("."), "t.csv"), "t", delimiter="|")
#     cur.execute("select count(*) from t")
#     res = cur.fetchall()[0][0]
#     if res != 2000:
#         raise Exception("expected to get 2000, instead got {}".format(res))
