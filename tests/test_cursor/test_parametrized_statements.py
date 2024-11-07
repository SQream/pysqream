from datetime import date, datetime
from decimal import Decimal
from random import randint

import pytest

from tests.test_base import TestBaseParameterizedStatements, DEFAULT_ARRAY_ELEMENTS_AMOUNT


class TestSelect(TestBaseParameterizedStatements):
    params = [1, 5, TestBaseParameterizedStatements.TEMP_TABLE_START_ROWS_AMOUNT]
    expected_result = [(i, False) for i in range(1, TestBaseParameterizedStatements.TEMP_TABLE_START_ROWS_AMOUNT + 1)]

    @pytest.mark.parametrize(
        "compare_symbol",
        (">", "<", ">=", "<=", "="),
        ids=("greater", "less", "greater_equal", "less_equal", "equal")
    )
    @pytest.mark.parametrize("param", params)
    def test_one_placeholder_all_comparison(self, sqream_cursor, param: int, compare_symbol):
        sqream_cursor.execute(f"select i, b from {self.TEMP_TABLE_NAME} where i {compare_symbol} ?",
                              params=[(param,)])
        if compare_symbol == "<":
            expected_result = self.expected_result[:param - 1]
        elif compare_symbol == ">":
            expected_result = self.expected_result[param:]
        elif compare_symbol == "<=":
            expected_result = self.expected_result[:param]
        elif compare_symbol == ">=":
            expected_result = self.expected_result[param - 1:]
        else:
            expected_result = [self.expected_result[param - 1]]
        assert sqream_cursor.fetchall() == expected_result

    def test_like_one_placeholder(self, sqream_cursor):
        sqream_cursor.execute(f"select t from {self.TEMP_TABLE_NAME} where t like ?", params=[('%text',)])
        assert sqream_cursor.fetchall() == [(f"{i} text",) for i in range(1, self.TEMP_TABLE_START_ROWS_AMOUNT + 1)]

        sqream_cursor.execute(f"select t, tar from {self.TEMP_TABLE_NAME} where tar[1] like ?",
                              params=[('1%',)])
        assert sqream_cursor.fetchall() == [("1 text", ["1 text", "1 text", "1 text"])]

    def test_in_one_placeholder(self, sqream_cursor):
        sqream_cursor.execute(f"select i, b from {self.TEMP_TABLE_NAME} where i in (?)", params=[(1,)])
        assert sqream_cursor.fetchall() == [(1, False)]

    def test_no_result(self, sqream_cursor):
        sqream_cursor.execute(f"select * from {self.TEMP_TABLE_NAME} where i = ?", params=[(10,)])
        assert sqream_cursor.fetchall() == []

    def test_int(self, sqream_cursor):
        index = randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT)
        sqream_cursor.execute(f"select * from {self.TEMP_TABLE_NAME} where i = ?", params=[(index,)])
        assert sqream_cursor.fetchall() == [self.generate_row(index)]

    def test_bool(self, sqream_cursor):
        sqream_cursor.execute(f"select * from {self.TEMP_TABLE_NAME} where b = ?", params=[(False,)])
        assert sqream_cursor.fetchall() == [self.generate_row(i) for i in range(1, 10)]

    def test_numeric(self, sqream_cursor):
        index = randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT)
        numeric = Decimal(self.RANDOM_NUMERIC.format(index=index))
        sqream_cursor.execute(f"select * from {self.TEMP_TABLE_NAME} where n = ?", params=[(numeric,)])
        assert sqream_cursor.fetchall() == [self.generate_row(index)]

    def test_date(self, sqream_cursor):
        index = randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT)
        row = self.generate_row(index=index)
        d = row[3]
        sqream_cursor.execute(f"select * from {self.TEMP_TABLE_NAME} where d = ?", params=[(d,)])
        assert sqream_cursor.fetchall() == [row]

    def test_datetime(self, sqream_cursor):
        index = randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT)
        row = self.generate_row(index=index)
        dt = row[4]
        sqream_cursor.execute(f"select * from {self.TEMP_TABLE_NAME} where dt = ?", params=[(dt,)])
        assert sqream_cursor.fetchall() == [row]

    def test_text(self, sqream_cursor):
        index = randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT)
        t = self.RANDOM_TEXT.format(index=index)
        sqream_cursor.execute(f"select * from {self.TEMP_TABLE_NAME} where t = ?", params=[(t,)])
        assert sqream_cursor.fetchall() == [self.generate_row(index)]

    def test_cast(self, sqream_cursor):
        index = randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT)
        t = self.RANDOM_TEXT.format(index=index)
        sqream_cursor.execute(f"select * from {self.TEMP_TABLE_NAME} where t = ?::text", params=[(t,)])
        assert sqream_cursor.fetchall() == [self.generate_row(index)]

    def test_int_array(self, sqream_cursor):
        index = randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT)
        sqream_cursor.execute(f"select * from {self.TEMP_TABLE_NAME} where iar = ?::int[]",
                              params=[([index] * DEFAULT_ARRAY_ELEMENTS_AMOUNT,)])
        assert sqream_cursor.fetchall() == [self.generate_row(index)]

    def test_bool_array(self, sqream_cursor):
        sqream_cursor.execute(f"select * from {self.TEMP_TABLE_NAME} where bar = ?::bool[]",
                              params=[([True] * DEFAULT_ARRAY_ELEMENTS_AMOUNT,)])
        assert sqream_cursor.fetchall() == [self.generate_row(i) for i in range(1, 10)]

    def test_numeric_array(self, sqream_cursor):
        index = randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT)
        numeric = Decimal(self.RANDOM_NUMERIC.format(index=index))
        sqream_cursor.execute(f"select * from {self.TEMP_TABLE_NAME} where nar = ?::numeric(15,6)[]",
                              params=[([numeric] * DEFAULT_ARRAY_ELEMENTS_AMOUNT,)])
        assert sqream_cursor.fetchall() == [self.generate_row(index)]

    def test_date_array(self, sqream_cursor):
        index = randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT)
        row = self.generate_row(index=index)
        d = row[3]
        sqream_cursor.execute(f"select * from {self.TEMP_TABLE_NAME} where dar = ?::date[]",
                              params=[([d] * DEFAULT_ARRAY_ELEMENTS_AMOUNT,)])
        assert sqream_cursor.fetchall() == [row]

    def test_datetime_array(self, sqream_cursor):
        index = randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT)
        row = self.generate_row(index=index)
        dt = row[4]
        sqream_cursor.execute(f"select * from {self.TEMP_TABLE_NAME} where dtar = ?::datetime[]",
                              params=[([dt] * DEFAULT_ARRAY_ELEMENTS_AMOUNT,)])
        assert sqream_cursor.fetchall() == [row]

    def test_text_array(self, sqream_cursor):
        index = randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT)
        t = self.RANDOM_TEXT.format(index=index)
        sqream_cursor.execute(f"select * from {self.TEMP_TABLE_NAME} where tar = ?::text[]",
                              params=[([t] * DEFAULT_ARRAY_ELEMENTS_AMOUNT,)])
        assert sqream_cursor.fetchall() == [self.generate_row(index)]

    def test_null(self, sqream_cursor):
        sqream_cursor.execute("create or replace table t(x int)")
        sqream_cursor.execute("insert into t values(NULL)")
        sqream_cursor.execute(f"select * from t where x = ?", params=[(None,)])
        assert sqream_cursor.fetchall() == []


class TestInsert(TestBaseParameterizedStatements):

    def test_int(self, sqream_cursor):
        index = 10
        sqream_cursor.execute(f"insert into {self.TEMP_TABLE_NAME} (i) values (?)", params=[(index,)])
        sqream_cursor.execute(f"select i from {self.TEMP_TABLE_NAME} where i = ?", params=[(index,)])
        assert sqream_cursor.fetchall() == [(10,)]

    def test_bool(self, sqream_cursor):
        sqream_cursor.execute(f"insert into {self.TEMP_TABLE_NAME} (b) values (?)", params=[(True,)])
        sqream_cursor.execute(f"select b from {self.TEMP_TABLE_NAME} where b = ?", params=[(True,)])
        assert sqream_cursor.fetchall() == [(True,)]

    def test_numeric(self, sqream_cursor):
        n = Decimal(f"{randint(int(1e8), int(1e9-1))}.{randint(int(1e5), int(1e6-1))}")
        sqream_cursor.execute(f"insert into {self.TEMP_TABLE_NAME} (n) values (?)", params=[(n,)])
        sqream_cursor.execute(f"select n from {self.TEMP_TABLE_NAME} where n = ?", params=[(n,)])
        assert sqream_cursor.fetchall() == [(n,)]

    def test_date(self, sqream_cursor):
        d = date(2024, 9, 20)
        sqream_cursor.execute(f"insert into {self.TEMP_TABLE_NAME} (d) values (?)", params=[(d,)])
        sqream_cursor.execute(f"select d from {self.TEMP_TABLE_NAME} where d = ?", params=[(d,)])
        assert sqream_cursor.fetchall() == [(d,)]

    def test_datetime(self, sqream_cursor):
        dt = datetime(2024, 9, 20, 23, 30, 25, 123000)
        sqream_cursor.execute(f"insert into {self.TEMP_TABLE_NAME} (dt) values (?)", params=[(dt,)])
        sqream_cursor.execute(f"select dt from {self.TEMP_TABLE_NAME} where dt = ?", params=[(dt,)])
        assert sqream_cursor.fetchall() == [(dt,)]

    def test_text(self, sqream_cursor):
        t = "Ryan Gosling"
        sqream_cursor.execute(f"insert into {self.TEMP_TABLE_NAME} (t) values (?)", params=[(t,)])
        sqream_cursor.execute(f"select t from {self.TEMP_TABLE_NAME} where t = ?", params=[(t,)])
        assert sqream_cursor.fetchall() == [(t,)]

    def test_int_array(self, sqream_cursor):
        params = [([10] * DEFAULT_ARRAY_ELEMENTS_AMOUNT,)]
        sqream_cursor.execute(f"insert into {self.TEMP_TABLE_NAME} (iar) values (?)", params=params)
        sqream_cursor.execute(f"select iar from {self.TEMP_TABLE_NAME} where iar = ?::int[]", params=params)
        assert sqream_cursor.fetchall() == params

    def test_bool_array(self, sqream_cursor):
        params = [([False] * DEFAULT_ARRAY_ELEMENTS_AMOUNT,)]
        sqream_cursor.execute(f"insert into {self.TEMP_TABLE_NAME} (bar) values (?)", params=params)
        sqream_cursor.execute(f"select bar from {self.TEMP_TABLE_NAME} where bar = ?::bool[]", params=params)
        assert sqream_cursor.fetchall() == params

    def test_insert_several_columns_without_arrays(self, sqream_cursor):
        params = (10, Decimal('123.123000'), date(2020, 10, 12), 'kavabanga')
        sqream_cursor.execute(f"insert into {self.TEMP_TABLE_NAME} (i, n, d, t) values (?, ?, ?, ?)", params=[params])
        sqream_cursor.execute(f"select i, n, d, t from {self.TEMP_TABLE_NAME} where i = ?", params=[(params[0],)])
        data = sqream_cursor.fetchall()
        assert data[0] == params
        sqream_cursor.execute(f"delete from {self.TEMP_TABLE_NAME} where i = ?", params=[(params[0],)])


class TestUpdate(TestBaseParameterizedStatements):

    @pytest.mark.parametrize("new", (10, None))
    def test_int(self, sqream_cursor, new):
        old = randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT)
        sqream_cursor.execute(f"update {self.TEMP_TABLE_NAME} set i = ? where i = ?", params=[(new, old)])
        sqream_cursor.execute(f"select i from {self.TEMP_TABLE_NAME} where i = ?", params=[(new,)])
        assert sqream_cursor.fetchall() == ([(new,)] if new else [])

    @pytest.mark.parametrize("new", (True, None))
    def test_bool(self, sqream_cursor, new):
        sqream_cursor.execute(f"update {self.TEMP_TABLE_NAME} set b = ? where b = ?", params=[(new, False)])
        sqream_cursor.execute(f"select b from {self.TEMP_TABLE_NAME} where b = ?", params=[(new,)])
        assert sqream_cursor.fetchall() == ([(new,)] * self.TEMP_TABLE_START_ROWS_AMOUNT if new else [])

    @pytest.mark.parametrize("new", (Decimal(f"{randint(int(1e8), int(1e9-1))}.{randint(int(1e5), int(1e6-1))}"), None))
    def test_numeric(self, sqream_cursor, new):
        old = Decimal(self.RANDOM_NUMERIC.format(index=randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT)))
        sqream_cursor.execute(f"update {self.TEMP_TABLE_NAME} set n = ? where n = ?",params=[(new, old)])
        sqream_cursor.execute(f"select n from {self.TEMP_TABLE_NAME} where n = ?", params=[(new,)])
        assert sqream_cursor.fetchall() == ([(new,)] if new else [])

    @pytest.mark.parametrize("new", (date(2024, 9, 20), None))
    def test_date(self, sqream_cursor, new):
        old = date.fromisoformat(self.RANDOM_DATE.format(index=randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT)))
        sqream_cursor.execute(f"update {self.TEMP_TABLE_NAME} set d = ? where d = ?", params=[(new, old)])
        sqream_cursor.execute(f"select d from {self.TEMP_TABLE_NAME} where d = ?", params=[(new,)])
        assert sqream_cursor.fetchall() == ([(new,)] if new else [])

    @pytest.mark.parametrize("new", (datetime(2024, 9, 20, 23, 30, 25, 123000), None))
    def test_datetime(self, sqream_cursor, new):
        old = datetime.strptime(self.RANDOM_DATETIME.format(index=randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT)),
                                "%Y-%m-%d %H:%M:%S.%f")
        sqream_cursor.execute(f"update {self.TEMP_TABLE_NAME} set dt = ? where dt = ?", params=[(new, old)])
        sqream_cursor.execute(f"select dt from {self.TEMP_TABLE_NAME} where dt = ?", params=[(new,)])
        assert sqream_cursor.fetchall() == ([(new,)] if new else [])

    @pytest.mark.parametrize("new", ("Ryan Gosling", None))
    def test_text(self, sqream_cursor, new):
        old = self.RANDOM_TEXT.format(index=randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT))
        sqream_cursor.execute(f"update {self.TEMP_TABLE_NAME} set t = ? where t = ?", params=[(new, old)])
        sqream_cursor.execute(f"select t from {self.TEMP_TABLE_NAME} where t = ?", params=[(new,)])
        assert sqream_cursor.fetchall() == ([(new,)] if new else [])

    @pytest.mark.parametrize("new", (10, None))
    def test_tinyint(self, sqream_cursor, new):
        old = randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT)
        sqream_cursor.execute(f"update {self.TEMP_TABLE_NAME} set ti = ? where ti = ?", params=[(new, old)])
        sqream_cursor.execute(f"select ti from {self.TEMP_TABLE_NAME} where ti = ?", params=[(new,)])
        assert sqream_cursor.fetchall() == ([(new,)] if new else [])

    @pytest.mark.parametrize("new", (10, None))
    def test_smallint(self, sqream_cursor, new):
        old, new = randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT), 10
        sqream_cursor.execute(f"update {self.TEMP_TABLE_NAME} set si = ? where si = ?", params=[(new, old)])
        sqream_cursor.execute(f"select si from {self.TEMP_TABLE_NAME} where si = ?", params=[(new,)])
        assert sqream_cursor.fetchall() == ([(new,)] if new else [])

    @pytest.mark.parametrize("new", (10, None))
    def test_bigint(self, sqream_cursor, new):
        old, new = randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT), 10
        sqream_cursor.execute(f"update {self.TEMP_TABLE_NAME} set bi = ? where bi = ?", params=[(new, old)])
        sqream_cursor.execute(f"select bi from {self.TEMP_TABLE_NAME} where bi = ?", params=[(new,)])
        assert sqream_cursor.fetchall() == ([(new,)] if new else [])

    @pytest.mark.parametrize("new", (10.1, None))
    def test_float(self, sqream_cursor, new):
        old = float(self.RANDOM_FLOAT.format(index=randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT)))
        sqream_cursor.execute(f"update {self.TEMP_TABLE_NAME} set f = ? where f = ?", params=[(new, old)])
        sqream_cursor.execute(f"select f from {self.TEMP_TABLE_NAME} where f = ?", params=[(new,)])
        assert sqream_cursor.fetchall() == ([(new,)] if new else [])

    @pytest.mark.parametrize("new", (10.1, None))
    def test_double(self, sqream_cursor, new):
        old = float(self.RANDOM_DOUBLE.format(index=randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT)))
        sqream_cursor.execute(f"update {self.TEMP_TABLE_NAME} set db = ? where db = ?", params=[(new, old)])
        sqream_cursor.execute(f"select db from {self.TEMP_TABLE_NAME} where db = ?", params=[(new,)])
        assert sqream_cursor.fetchall() == ([(new,)] if new else [])

    @pytest.mark.parametrize("new", (10, None))
    def test_real(self, sqream_cursor, new):
        old = float(self.RANDOM_REAL.format(index=randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT)))
        sqream_cursor.execute(f"update {self.TEMP_TABLE_NAME} set r = ? where r = ?", params=[(new, old)])
        sqream_cursor.execute(f"select r from {self.TEMP_TABLE_NAME} where r = ?", params=[(new,)])
        assert sqream_cursor.fetchall() ==  ([(10.0,)] if new else [])

    def test_cast(self, sqream_cursor):
        old, new = randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT), 10
        sqream_cursor.execute(f"update {self.TEMP_TABLE_NAME} set i = ? where i = ?::int", params=[(new, old)])
        sqream_cursor.execute(f"select i from {self.TEMP_TABLE_NAME} where i = ?", params=[(new,)])
        assert sqream_cursor.fetchall() == [(new,)]


class TestDelete(TestBaseParameterizedStatements):

    @pytest.fixture
    def check_number_of_rows_has_decreased(self, sqream_cursor):
        sqream_cursor.execute(f"select count(*) from {self.TEMP_TABLE_NAME}")
        rows_amount_before = sqream_cursor.fetchone()[0]
        yield
        sqream_cursor.execute(f"select count(*) from {self.TEMP_TABLE_NAME}")
        rows_amount_after = sqream_cursor.fetchone()[0]
        assert rows_amount_before > rows_amount_after, (f"Rows amount before must be greater than "
                                                        f"after: `{rows_amount_before}` !> `{rows_amount_after}`")

    def test_int(self, sqream_cursor, check_number_of_rows_has_decreased):
        index = randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT)
        sqream_cursor.execute(f"delete from {self.TEMP_TABLE_NAME} where i = ?", params=[(index,)])
        sqream_cursor.execute(f"select i from {self.TEMP_TABLE_NAME} where i = ?", params=[(index,)])
        assert sqream_cursor.fetchall() == []

    def test_bool(self, sqream_cursor, check_number_of_rows_has_decreased):
        sqream_cursor.execute(f"delete from {self.TEMP_TABLE_NAME} where b = ?", params=[(False,)])
        sqream_cursor.execute(f"select * from {self.TEMP_TABLE_NAME}")
        assert sqream_cursor.fetchall() == []

    def test_numeric(self, sqream_cursor, check_number_of_rows_has_decreased):
        n = Decimal(self.RANDOM_NUMERIC.format(index=randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT)))
        sqream_cursor.execute(f"delete from {self.TEMP_TABLE_NAME} where n = ?", params=[(n,)])
        sqream_cursor.execute(f"select n from {self.TEMP_TABLE_NAME} where n = ?", params=[(n,)])
        assert sqream_cursor.fetchall() == []

    def test_date(self, sqream_cursor, check_number_of_rows_has_decreased):
        d = date.fromisoformat(self.RANDOM_DATE.format(index=1))
        sqream_cursor.execute(f"delete from {self.TEMP_TABLE_NAME} where d = ?", params=[(d,)])
        sqream_cursor.execute(f"select d from {self.TEMP_TABLE_NAME} where d = ?", params=[(d,)])
        assert sqream_cursor.fetchall() == []

    def test_datetime(self, sqream_cursor, check_number_of_rows_has_decreased):
        dt = datetime.strptime(TestBaseParameterizedStatements.RANDOM_DATETIME.format(index=1), "%Y-%m-%d %H:%M:%S.%f")
        sqream_cursor.execute(f"delete from {self.TEMP_TABLE_NAME} where dt = ?", params=[(dt,)])
        sqream_cursor.execute(f"select dt from {self.TEMP_TABLE_NAME} where dt = ?", params=[(dt,)])
        assert sqream_cursor.fetchall() == []

    def test_text(self, sqream_cursor, check_number_of_rows_has_decreased):
        t = self.RANDOM_TEXT.format(index=randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT))
        sqream_cursor.execute(f"delete from {self.TEMP_TABLE_NAME} where t = ?", params=[(t,)])
        sqream_cursor.execute(f"select t from {self.TEMP_TABLE_NAME} where t = ?", params=[(t,)])
        assert sqream_cursor.fetchall() == []

    def test_tinyint(self, sqream_cursor, check_number_of_rows_has_decreased):
        index = randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT)
        sqream_cursor.execute(f"delete from {self.TEMP_TABLE_NAME} where ti = ?", params=[(index,)])
        sqream_cursor.execute(f"select ti from {self.TEMP_TABLE_NAME} where ti = ?", params=[(index,)])
        assert sqream_cursor.fetchall() == []

    def test_smallint(self, sqream_cursor, check_number_of_rows_has_decreased):
        index = randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT)
        sqream_cursor.execute(f"delete from {self.TEMP_TABLE_NAME} where si = ?", params=[(index,)])
        sqream_cursor.execute(f"select si from {self.TEMP_TABLE_NAME} where si = ?", params=[(index,)])
        assert sqream_cursor.fetchall() == []

    def test_bigint(self, sqream_cursor, check_number_of_rows_has_decreased):
        index = randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT)
        sqream_cursor.execute(f"delete from {self.TEMP_TABLE_NAME} where bi = ?", params=[(index,)])
        sqream_cursor.execute(f"select bi from {self.TEMP_TABLE_NAME} where bi = ?", params=[(index,)])
        assert sqream_cursor.fetchall() == []

    def test_float(self, sqream_cursor, check_number_of_rows_has_decreased):
        index = float(self.RANDOM_FLOAT.format(index=randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT)))
        sqream_cursor.execute(f"delete from {self.TEMP_TABLE_NAME} where f = ?", params=[(index,)])
        sqream_cursor.execute(f"select f from {self.TEMP_TABLE_NAME} where f = ?", params=[(index,)])
        assert sqream_cursor.fetchall() == []

    def test_double(self, sqream_cursor, check_number_of_rows_has_decreased):
        index = float(self.RANDOM_DOUBLE.format(index=randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT)))
        sqream_cursor.execute(f"delete from {self.TEMP_TABLE_NAME} where db = ?", params=[(index,)])
        sqream_cursor.execute(f"select db from {self.TEMP_TABLE_NAME} where db = ?", params=[(index,)])
        assert sqream_cursor.fetchall() == []

    def test_real(self, sqream_cursor, check_number_of_rows_has_decreased):
        index = float(self.RANDOM_REAL.format(index=randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT)))
        sqream_cursor.execute(f"delete from {self.TEMP_TABLE_NAME} where r = ?", params=[(index,)])
        sqream_cursor.execute(f"select r from {self.TEMP_TABLE_NAME} where r = ?", params=[(index,)])
        assert sqream_cursor.fetchall() == []

    def test_int_null(self, sqream_cursor):
        sqream_cursor.execute(f"delete from {self.TEMP_TABLE_NAME} where i = ?", params=[(None,)])
        sqream_cursor.execute(f"select i from {self.TEMP_TABLE_NAME} where i = ?", params=[(None,)])
        assert sqream_cursor.fetchall() == []

    def test_bool_null(self, sqream_cursor):
        sqream_cursor.execute(f"delete from {self.TEMP_TABLE_NAME} where b = ?", params=[(None,)])
        sqream_cursor.execute(f"select * from {self.TEMP_TABLE_NAME}")
        assert sqream_cursor.fetchall() == []

    def test_numeric_null(self, sqream_cursor):
        sqream_cursor.execute(f"delete from {self.TEMP_TABLE_NAME} where n = ?", params=[(None,)])
        sqream_cursor.execute(f"select n from {self.TEMP_TABLE_NAME} where n = ?", params=[(None,)])
        assert sqream_cursor.fetchall() == []

    def test_date_null(self, sqream_cursor):
        sqream_cursor.execute(f"delete from {self.TEMP_TABLE_NAME} where d = ?", params=[(None,)])
        sqream_cursor.execute(f"select d from {self.TEMP_TABLE_NAME} where d = ?", params=[(None,)])
        assert sqream_cursor.fetchall() == []

    def test_datetime_null(self, sqream_cursor):
        sqream_cursor.execute(f"delete from {self.TEMP_TABLE_NAME} where dt = ?", params=[(None,)])
        sqream_cursor.execute(f"select dt from {self.TEMP_TABLE_NAME} where dt = ?", params=[(None,)])
        assert sqream_cursor.fetchall() == []

    def test_text_null(self, sqream_cursor):
        sqream_cursor.execute(f"delete from {self.TEMP_TABLE_NAME} where t = ?", params=[(None,)])
        sqream_cursor.execute(f"select t from {self.TEMP_TABLE_NAME} where t = ?", params=[(None,)])
        assert sqream_cursor.fetchall() == []

    def test_tinyint_null(self, sqream_cursor):
        sqream_cursor.execute(f"delete from {self.TEMP_TABLE_NAME} where ti = ?", params=[(None,)])
        sqream_cursor.execute(f"select ti from {self.TEMP_TABLE_NAME} where ti = ?", params=[(None,)])
        assert sqream_cursor.fetchall() == []

    def test_smallint_null(self, sqream_cursor):
        sqream_cursor.execute(f"delete from {self.TEMP_TABLE_NAME} where si = ?", params=[(None,)])
        sqream_cursor.execute(f"select si from {self.TEMP_TABLE_NAME} where si = ?", params=[(None,)])
        assert sqream_cursor.fetchall() == []

    def test_bigint_null(self, sqream_cursor):
        sqream_cursor.execute(f"delete from {self.TEMP_TABLE_NAME} where bi = ?", params=[(None,)])
        sqream_cursor.execute(f"select bi from {self.TEMP_TABLE_NAME} where bi = ?", params=[(None,)])
        assert sqream_cursor.fetchall() == []

    def test_float_null(self, sqream_cursor):
        sqream_cursor.execute(f"delete from {self.TEMP_TABLE_NAME} where f = ?", params=[(None,)])
        sqream_cursor.execute(f"select f from {self.TEMP_TABLE_NAME} where f = ?", params=[(None,)])
        assert sqream_cursor.fetchall() == []

    def test_double_null(self, sqream_cursor):
        sqream_cursor.execute(f"delete from {self.TEMP_TABLE_NAME} where db = ?", params=[(None,)])
        sqream_cursor.execute(f"select db from {self.TEMP_TABLE_NAME} where db = ?", params=[(None,)])
        assert sqream_cursor.fetchall() == []

    def test_real_null(self, sqream_cursor):
        sqream_cursor.execute(f"delete from {self.TEMP_TABLE_NAME} where db = ?", params=[(None,)])
        sqream_cursor.execute(f"select db from {self.TEMP_TABLE_NAME} where db = ?", params=[(None,)])
        assert sqream_cursor.fetchall() == []

    def test_cast(self, sqream_cursor):
        index = randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT)
        sqream_cursor.execute(f"delete from {self.TEMP_TABLE_NAME} where i = ?::int", params=[(index,)])
        sqream_cursor.execute(f"select i from {self.TEMP_TABLE_NAME} where i = ?", params=[(index,)])
        assert sqream_cursor.fetchall() == []

    def test_multiple_params(self, sqream_cursor):
        sqream_cursor.execute(f"delete from {self.TEMP_TABLE_NAME} where i = ? or i = ?", params=[(1, 2)])
        sqream_cursor.execute(f"select i from {self.TEMP_TABLE_NAME} where i = ? or i = ?", params=[(1, 2)])
        assert sqream_cursor.fetchall() == []


class TestNegative(TestBaseParameterizedStatements):

    def test_select_placeholders_in_wrong_places(self, sqream_cursor):
        with pytest.raises(Exception) as parameterized_error:
            sqream_cursor.execute(f"select * from ? where i = ?", params=[(1, 2)])
        assert "Parsing error in statement line" in str(parameterized_error)

    def test_select_placeholders_in_wrong_places(self, sqream_cursor):
        with pytest.raises(Exception) as parameterized_error:
            sqream_cursor.execute(f"SELECT * FROM {self.TEMP_TABLE_NAME} WHERE (? IS NULL AND i IS NULL) OR i = ?;", params=[(1, 2)])
        assert "Unsupported parametrized query" in str(parameterized_error)

