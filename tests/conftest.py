"""Global test configurations and fixtures"""
import pytest

from pysqream.pysqream import connect

from tests.test_base import Logger


DEFAULT_IP = "192.168.0.35"
DEFAULT_SQREAM_PORT = "5000"
DEFAULT_PICKER_PORT = "3108"
DEFAULT_DATABASE = "master"
DEFAULT_USERNAME = "sqream"
DEFAULT_PASSWORD = "sqream"
DEFAULT_CLUSTERED = False
DEFAULT_USR_SSL = False
logger = Logger()


def pytest_addoption(parser):
    parser.addoption("--ip", action="store", help="SQream Server ip", default=DEFAULT_IP)
    parser.addoption("--port", action="store", help="SQream Server port", default=DEFAULT_SQREAM_PORT)
    parser.addoption("--picker_port", action="store", help="SQream Server picker port", default=DEFAULT_PICKER_PORT)
    parser.addoption("--database", action="store", help="SQream Server database", default=DEFAULT_DATABASE)
    parser.addoption("--username", action="store", help="SQream Server username", default=DEFAULT_USERNAME)
    parser.addoption("--password", action="store", help="SQream Server password", default=DEFAULT_PASSWORD)
    parser.addoption("--clustered", action="store", help="SQream Server clustered", default=DEFAULT_CLUSTERED)
    parser.addoption("--use_ssl", action="store", help="SQream Server use_ssl", default=DEFAULT_USR_SSL)


def pytest_generate_tests(metafunc):
    metafunc.config.getoption("ip")


@pytest.fixture(scope='session', name='ip')
def sqream_ip_address(pytestconfig):
    """Fixture that adopts IP address of SQream Server for direct tests"""
    yield pytestconfig.getoption("ip")


@pytest.fixture(scope='session', name='port')
def sqream_port_number(pytestconfig):
    """Fixture that adopts PORT of SQream Server for direct tests"""
    try:
        port = int(pytestconfig.getoption("port"))
    except ValueError as exc:
        raise ValueError("Please provide correct PORT type") from exc
    yield port


@pytest.fixture(scope='session', name='picker_port')
def sqream_picker_port_number(pytestconfig):
    """Fixture that adopts PORT of SQream Picker Server for direct tests"""
    try:
        port = int(pytestconfig.getoption("picker_port"))
    except ValueError as exc:
        raise ValueError("Please provide correct PICKER PORT type") from exc
    yield port


@pytest.fixture(scope='session', name='database')
def sqream_database(pytestconfig):
    """Fixture that adopts DATABASE Name of SQream Server for direct tests"""
    yield pytestconfig.getoption("database")


@pytest.fixture(scope='session', name='username')
def sqream_username(pytestconfig):
    """Fixture that adopts USERNAME of SQream Server for direct tests"""
    yield pytestconfig.getoption("username")


@pytest.fixture(scope='session', name='password')
def sqream_password(pytestconfig):
    """Fixture that adopts PASSWORD of SQream Server for direct tests"""
    yield pytestconfig.getoption("password")


@pytest.fixture(scope='session', name='clustered')
def sqream_clustered(pytestconfig):
    """Fixture that adopts CLUSTERED of SQream Server for direct tests"""
    if str(pytestconfig.getoption("clustered")).lower() in ['True', 'true', 1]:
        clustered = True
    elif str(pytestconfig.getoption("clustered")).lower() in ['False', 'false', 0]:
        clustered = False
    else:
        raise ValueError("lease provide correct CLUSTERED type")
    yield clustered


@pytest.fixture(scope='session', name='use_ssl')
def sqream_use_ssl(pytestconfig):
    """Fixture that adopts PASSWORD of SQream Server for direct tests"""
    if str(pytestconfig.getoption("use_ssl")).lower() in ['True', 'true', 1]:
        use_ssl = True
    elif str(pytestconfig.getoption("use_ssl")).lower() in ['False', 'false', 0]:
        use_ssl = False
    else:
        raise ValueError("lease provide correct USE SSL type")
    yield use_ssl

@pytest.fixture(name='conn')
def sqream_connection(ip, port):  # pylint: disable=invalid-name
    """Fixture that create connection at each direct fixture call"""
    connection = connect(host=ip,
                         port=port,
                         database='master',
                         username='sqream',
                         password='sqream',
                         clustered=False,
                         use_ssl=False)
    yield connection
    connection.close()


@pytest.fixture()
def sqream_cursor(conn):
    """Fixture that create cursor at each direct fixture call"""

    cur = conn.cursor()
    yield cur
    if not cur.closed:
        cur.close()
