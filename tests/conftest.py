"""Global test configurations and fixtures"""
import pytest

import pysqream

from base import Logger


DEFAULT_IP = "192.168.0.35"
logger = Logger()


def pytest_addoption(parser):
    parser.addoption(
        "--ip", action="store", help="SQream Server ip", default=DEFAULT_IP)
    parser.addoption(
        "--port", action="store", help="SQream Server port", default="5000")


def pytest_generate_tests(metafunc):
    metafunc.config.getoption("ip")


@pytest.fixture(scope='session', name='ip_address')
def sqream_ip_address(pytestconfig):
    """Fixture that adopts IP address of SQream Server for direct tests"""
    yield pytestconfig.getoption("ip")


@pytest.fixture(scope='session', name='port')
def sqream_port_number(pytestconfig):
    """Fixture that adopts PORT of SQream Server for direct tests"""
    try:
        port = int(pytestconfig.getoption("port"))
    except ValueError as exc:
        raise ValueError("Please provide correct IP address") from exc
    yield port


@pytest.fixture(name='conn')
def sqream_connection(ip_address, port):
    """Fixture that create connection at each direct fixture call"""
    conn = pysqream.connect(
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
