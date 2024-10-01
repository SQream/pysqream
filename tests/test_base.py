from decimal import Decimal
from os import PathLike
from pathlib import Path
from datetime import date, datetime

import pytest
import socket

import pysqream
from tests.pytest_logger import Logger


logger = Logger()
DEFAULT_ARRAY_ELEMENTS_AMOUNT = 3


def connect_dbapi(ip, clustered=False, use_ssl=False, port=5000, picker_port=3108,
                  database='master', username='sqream', password='sqream'):
    if clustered:
        port = picker_port
    logger.info(f"Connect to server {ip}:{port}/database={database};username={username};password={password.replace(password, '*' * len(password))};"
                  f"clustered={clustered};use_ssl={use_ssl}")
    return pysqream.connect(ip, port, database, username, password, clustered, use_ssl)


class TestBase:

    @pytest.fixture
    def big_data_ddl_path(self) -> PathLike:
        return (Path(__file__).parent / "big_data.ddl").absolute()

    @pytest.fixture(autouse=True)
    def Test_setup_teardown(self, ip, clustered, use_ssl, port, picker_port, database,
                            username, password):
        ip = ip if ip else socket.gethostbyname(socket.gethostname())
        logger.info("Before Scenario")
        self.con = connect_dbapi(ip, clustered=clustered, use_ssl=use_ssl, port=port, picker_port=picker_port,
                                 database=database, username=username, password=password)
        yield
        logger.info("After Scenario")
        self.con.close()
        logger.info(f"Close Session to server {ip}")


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


class TestBaseParameterizedStatements:
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
    RANDOM_DATETIME = "200{index}-01-10 12:00:00.{index}23000"
    RANDOM_TEXT = "{index} text"
    DEFAULT_PLACEHOLDER = "?"

    def generate_row(self, index: int, array_length: int = DEFAULT_ARRAY_ELEMENTS_AMOUNT) -> tuple[
                int, bool, Decimal, date, datetime, str,
                list[int], list[bool], list[Decimal], list[date], list[datetime], list[str]
            ]:
        n = Decimal(self.RANDOM_NUMERIC.format(index=index))
        d = date.fromisoformat(self.RANDOM_DATE.format(index=index))
        dt = datetime.strptime(self.RANDOM_DATETIME.format(index=index), "%Y-%m-%d %H:%M:%S.%f")
        t = self.RANDOM_TEXT.format(index=index)
        iar = [index] * array_length
        bar = [True] * array_length
        nar = [n] * array_length
        dar = [d] * array_length
        dtar = [dt] * array_length
        tar = [t] * array_length
        return index, False, n, d, dt, t, iar, bar, nar, dar, dtar, tar

    @pytest.fixture(autouse=True)
    def create_temp_table_and_insert(self, sqream_cursor):
        """This fixture creates `TEMP_TABLE_NAME` with `TEMP_TABLE_COLUMNS` and insert `TEMP_TABLE_START_ROWS_AMOUNT`
        rows inside
        """
        sqream_cursor.execute(f"create or replace table {self.TEMP_TABLE_NAME} ({', '.join(self.TEMP_TABLE_COLUMNS)})")
        for index in range(1, self.TEMP_TABLE_START_ROWS_AMOUNT + 1):
            placeholders = ", ".join([self.DEFAULT_PLACEHOLDER for _ in range(len(self.TEMP_TABLE_COLUMNS))])
            params = self.generate_row(index=index)
            sqream_cursor.execute(f"insert into {self.TEMP_TABLE_NAME} values ({placeholders})", params=[params])
        yield
        sqream_cursor.execute(f"drop table {self.TEMP_TABLE_NAME}")
