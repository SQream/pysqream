"""Global test configurations and fixtures"""
import os
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


@pytest.fixture(scope='session', name='worker_id')
def ensure_worker_id(request):
    """Provide worker_id even when pytest-xdist is not installed"""
    try:
        value = request.getfixturevalue('worker_id')
    except pytest.FixtureLookupError:
        value = "master"
    yield value


def pytest_configure(config):
    """
    Configure separate log_files for workers

    Cookbook: https://pytest-xdist.readthedocs.io/en/latest/how-to.html
    """
    worker_id = os.environ.get("PYTEST_XDIST_WORKER")
    log_file = config.getini("log_file")
    if worker_id is not None and log_file:
        filename, file_extension = os.path.splitext(log_file)
        logging.basicConfig(
            format=config.getini("log_file_format"),
            filename=f"{filename}_{worker_id}{file_extension}",
            level=config.getini("log_file_level") or "NOTSET",
        )


def pytest_addoption(parser):
    """Pytest way for adding options"""
    parser.addoption(
        "--ip", action="store", help="SQream Server ip", default=DEFAULT_IP)
    parser.addoption(
        "--port", action="store", help="SQream Server port", default="5000")
    parser.addoption(
        "--cport", action="store", help="SQream Server clustered port",
        default="3108")
    parser.addoption(
        "--recreate", action="store_true",
        help="DROPs database if exists before tests", default=False)


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


@pytest.fixture(scope="session", name="db_name")
def create_unique_database(pytestconfig, base_conn, worker_id):
    """Creates database for testing for master or for each worker"""
    recreate = pytestconfig.getoption("recreate")
    db_name = f"test_pysqream_testrun_worker_{worker_id}"
    logger.warning("Create database for worker (%s): %s", worker_id, db_name)
    cursor = base_conn.cursor()
    if recreate:
        try:
            cursor.execute(f"DROP DATABASE {db_name}")
        except Exception as exc:  # pylint: disable=W0718
            if f"Database '{db_name}' not found" not in str(exc):
                raise
        else:
            logger.warning("Previous database is dropped")
    try:
        cursor.execute(f"CREATE DATABASE {db_name}")
    except Exception as exc:
        msg = "Couldn't create database"
        if 'already exists' in str(exc):
            msg += ", delete it first"
        logger.error(msg)
        raise

    yield db_name
    logger.warning("Drop database for worker (%s): %s", worker_id, db_name)
    cursor.execute(f"DROP DATABASE {db_name}")


@pytest.fixture(scope="session", name='base_conn')
def sqream_base_connection(ip_address, port):
    """Fixture that create connection at each direct fixture call"""
    conn = connect(
        ip_address, port, 'master', 'sqream', 'sqream', False, False)
    yield conn
    conn.close()


@pytest.fixture(name='conn')
def sqream_connection(ip_address, port, db_name):
    """Fixture that create connection at each direct fixture call"""
    conn = connect(ip_address, port, db_name, 'sqream', 'sqream', False, False)
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
