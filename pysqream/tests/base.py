import pytest
import socket
import sys
import os
sys.path.append(os.path.abspath(__file__).rsplit('tests/', 1)[0] + '/tests/')
from pysqream.pysqream import connect
from pysqream.tests.pytest_logger import Logger


def connect_dbapi(ip, clustered=False, use_ssl=False):
    port = (3109 if use_ssl else 3108) if clustered else (5001 if use_ssl else 5000)
    return connect(ip, port, 'master', 'sqream', 'sqream', clustered, use_ssl)


class TestBase():

    @pytest.fixture()
    def ip(self, pytestconfig):
        return pytestconfig.getoption("ip")

    @pytest.fixture(autouse=True)
    def Test_setup_teardown(self, ip):
        ip = ip if ip else socket.gethostbyname(socket.gethostname())
        Logger().info("Before Scenario")
        Logger().info(f"Connect to server {ip}")
        self.con = connect_dbapi(ip)
        yield
        Logger().info("After Scenario")
        self.con.close()
        Logger().info(f"Close Session to server {ip}")


class TestBaseWithoutBeforeAfter():
    @pytest.fixture()
    def ip(self, pytestconfig):
        return pytestconfig.getoption("ip")

    @pytest.fixture(autouse=True)
    def Test_setup_teardown(self, ip):
        self.ip = ip if ip else socket.gethostbyname(socket.gethostname())
        yield