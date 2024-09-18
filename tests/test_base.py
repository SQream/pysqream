from os import PathLike
from pathlib import Path
from random import *

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


class TestBaseParametrizedStatements:
    """Draft test plan:
    https://sqream.atlassian.net/l/cp/TF7xxqxr
    """

    TEMP_TABLE_NAME: str = "pst"
    # Be aware of setting next constant greater than 10 because it will provide date and other stuff like `20010-01-10`
    # instead of `2009-01-10`
    TEMP_TABLE_START_ROWS_AMOUNT: int = 9
    TEMP_TABLE_COLUMNS: str = ("i int", "b bool", "n numeric(15, 6)", "d date", "dt datetime", "t text",
                               "iar int[]", "bar bool[]", "nar numeric(15, 6)[]", "dar date[]", "dtar datetime[]",
                               "tar text[]")
    RANDOM_NUMERIC = "{index}18496281.983903"
    RANDOM_DATE = "200{index}-01-10"
    RANDOM_DATETIME = "200{index}-01-10 12:00:00.{index}2345"
    RANDOM_TEXT = "{index} text"

    def generate_row(self, index: int) -> tuple[int, int, float, str, str, str, str, str, str, str, str, str]:
        n = float(self.RANDOM_NUMERIC.format(index=index))
        d = self.RANDOM_DATE.format(index=index)
        dt = self.RANDOM_DATETIME.format(index=index)
        t = self.RANDOM_TEXT.format(index=index)
        iar = f"array[{index}, {index}, {index}]"
        bar = "array[1, 0, 1]"
        nar = f"array[{n}, {n}, {n}]"
        dar = f"array['{d}', '{d}', '{d}']"
        dtar = f"array['{dt}', '{dt}', '{dt}']"
        tar = f"array['{t}', '{t}', '{t}']"
        return index, 0, n, d, dt, t, iar, bar, nar, dar, dtar, tar

    @pytest.fixture(autouse=True)
    def create_temp_table_and_insert(self, sqream_cursor):
        """This fixture creates `TEMP_TABLE_NAME` with `TEMP_TABLE_COLUMNS` and insert `TEMP_TABLE_START_ROWS_AMOUNT`
        rows inside
        """
        sqream_cursor.execute(f"create or replace table {self.TEMP_TABLE_NAME} ({', '.join(self.TEMP_TABLE_COLUMNS)})")
        for index in range(1, self.TEMP_TABLE_START_ROWS_AMOUNT + 1):
            i, b, n, d, dt, t, iar, bar, nar, dar, dtar, tar = self.generate_row(index=index)
            row = f"{i}, {b}, {n}, '{d}', '{dt}', '{t}', {iar}, {bar}, {nar}, {dar}, {dtar}, {tar}"
            sqream_cursor.execute(f"insert into {self.TEMP_TABLE_NAME} values ({row})")
        yield