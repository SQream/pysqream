"""Global test configurations and fixtures"""
import logging
import pytest

from pysqream.cursor import Cursor
from pysqream import connect


DEFAULT_IP = "192.168.0.35"
logger = logging.getLogger(__name__)


class ConnectionMock:
    """Mock of pysqream.connection.Connection to prevent real connection"""
    # pylint: disable=too-few-public-methods; they are not need for Mocks
    s = None
    version = 'Mock1'

    def close_connection(self):
        """Mock presence of close_connection method"""


def pytest_addoption(parser):
    """Pytest way for adding options"""
    parser.addoption(
        "--ip", action="store", help="SQream Server ip", default=DEFAULT_IP)
    parser.addoption(
        "--port", action="store", help="SQream Server port", default="5000")
    parser.addoption(
        "--cport", action="store", help="SQream Server clustered port",
        default="3108")


@pytest.fixture(scope='session', name='ip_address')
def sqream_ip_address(pytestconfig):
    """Fixture that adopts IP address of SQream Server for direct tests"""
    yield pytestconfig.getoption("ip")


@pytest.fixture(scope='session', name='ip')
def sqream_ip(ip_address):
    """The same as ip_address fixture, but using name ip for compatibility"""
    yield ip_address


@pytest.fixture(scope='session', name='port')
def sqream_port_number(pytestconfig):
    """Fixture that adopts PORT of SQream Server for direct tests"""
    try:
        port = int(pytestconfig.getoption("port"))
    except ValueError as exc:
        raise ValueError("Please provide correct IP address") from exc
    yield port


@pytest.fixture(scope='session', name='cport')
def sqream_clustered_port_number(pytestconfig):
    """Fixture that adopts PORT of SQream Server for direct tests"""
    try:
        port = int(pytestconfig.getoption("cport"))
    except ValueError as exc:
        raise ValueError(
            "Please provide correct IP address for clustered") from exc
    yield port


@pytest.fixture(name='conn')
def sqream_connection(ip_address, port):
    """Fixture that create connection at each direct fixture call"""
    conn = connect(
        ip_address, port, 'master', 'sqream', 'sqream', False, False)
    yield conn
    conn.close()


@pytest.fixture(name='cursor')
def sqream_cursor(conn):
    """Fixture that create cursor at each direct fixture call"""

    cur = conn.cursor()
    yield cur
    if not cur.closed:
        cur.close()


@pytest.fixture(name='mock_cursor')
def sqream_mock_cursor():
    """Fixture that create cursor with mocked connection"""
    cur = Cursor(ConnectionMock(), [])
    yield cur  # Don't need to close, because no real connection exists
