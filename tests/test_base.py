from os import PathLike
from pathlib import Path

import pytest
import socket

import pysqream
from tests.pytest_logger import Logger


def connect_dbapi(ip, clustered=False, use_ssl=False, port=5000, picker_port=3108,
                  database='master', username='sqream', password='sqream'):
    if clustered:
        port = picker_port
    Logger().info(f"Connect to server {ip}:{port}/database={database};username={username};password={password.replace(password, '*' * len(password))};"
                  f"clustered={clustered};use_ssl={use_ssl}")
    return pysqream.connect(ip, port, database, username, password, clustered, use_ssl)


class TestBase:

    @pytest.fixture
    def big_data_ddl_path(self) -> PathLike:
        return Path("tests/big_data.ddl").absolute()

    @pytest.fixture(autouse=True)
    def Test_setup_teardown(self, ip, clustered, use_ssl, port, picker_port, database,
                            username, password):
        ip = ip if ip else socket.gethostbyname(socket.gethostname())
        Logger().info("Before Scenario")
        self.con = connect_dbapi(ip, clustered=clustered, use_ssl=use_ssl, port=port, picker_port=picker_port,
                                 database=database, username=username, password=password)
        yield
        Logger().info("After Scenario")
        self.con.close()
        Logger().info(f"Close Session to server {ip}")


class TestBaseWithoutBeforeAfter:

    @pytest.fixture(autouse=True)
    def Test_setup_teardown(self, ip, clustered, use_ssl, port, picker_port,
                            database, username, password):
        self.ip = ip if ip else socket.gethostbyname(socket.gethostname())
        self.clustered = clustered
        self.use_ssl = use_ssl
        self.port = port
        self.picker_port = picker_port
        self.database = database
        self.username = username
        self.password = password
        # Due to metadata as limit of failure connection we reconnect for refresh the metadata counter
        if clustered:
            con = connect_dbapi(ip, clustered=clustered, use_ssl=use_ssl, port=port, picker_port=picker_port,
                          database=database, username=username, password=password)
            con.close()
        yield
