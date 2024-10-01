from datetime import date, datetime
from decimal import Decimal
from random import randint

import pytest

from tests.test_base import TestBaseParameterizedStatements, DEFAULT_ARRAY_ELEMENTS_AMOUNT


class TestSelect(TestBaseParameterizedStatements):
    params = (1, 5, TestBaseParameterizedStatements.TEMP_TABLE_START_ROWS_AMOUNT)
    expected_result = [(i, False) for i in range(1, TestBaseParameterizedStatements.TEMP_TABLE_START_ROWS_AMOUNT + 1)]

    @pytest.mark.parametrize(
        "compare_symbol",
        (">", "<", ">=", "<=", "="),
        ids=("greater", "less", "greater_equal", "less_equal", "equal")
    )
    @pytest.mark.parametrize("param", params)
    def test_one_placeholder_all_comparison(self, sqream_cursor, param: int, compare_symbol):
        sqream_cursor.execute(f"select i, b from {self.TEMP_TABLE_NAME} where i {compare_symbol} ?",
                              params=(param,))
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
        sqream_cursor.execute(f"select t from {self.TEMP_TABLE_NAME} where t like ?", params=('%text',))
        assert sqream_cursor.fetchall() == [(f"{i} text",) for i in range(1, self.TEMP_TABLE_START_ROWS_AMOUNT + 1)]

        sqream_cursor.execute(f"select t, tar from {self.TEMP_TABLE_NAME} where tar[1] like ?",
                              params=('1%',))
        assert sqream_cursor.fetchall() == [("1 text", ["1 text", "1 text", "1 text"])]

    def test_in_one_placeholder(self, sqream_cursor):
        sqream_cursor.execute(f"select i, b from {self.TEMP_TABLE_NAME} where i in (?)", params=(1,))
        assert sqream_cursor.fetchall() == [(1, False)]

    def test_int(self, sqream_cursor):
        index = randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT)
        sqream_cursor.execute(f"select * from {self.TEMP_TABLE_NAME} where i = ?", params=(index,))
        assert sqream_cursor.fetchall() == [self.generate_row(index)]

    def test_bool(self, sqream_cursor):
        sqream_cursor.execute(f"select * from {self.TEMP_TABLE_NAME} where b = ?", params=(False,))
        assert sqream_cursor.fetchall() == [self.generate_row(i) for i in range(1, 10)]

    def test_numeric(self, sqream_cursor):
        index = randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT)
        numeric = Decimal(self.RANDOM_NUMERIC.format(index=index))
        sqream_cursor.execute(f"select * from {self.TEMP_TABLE_NAME} where n = ?", params=(numeric,))
        assert sqream_cursor.fetchall() == [self.generate_row(index)]

    def test_date(self, sqream_cursor):
        index = randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT)
        row = self.generate_row(index=index)
        d = row[3]
        sqream_cursor.execute(f"select * from {self.TEMP_TABLE_NAME} where d = ?", params=(d,))
        assert sqream_cursor.fetchall() == [row]

    def test_datetime(self, sqream_cursor):
        index = randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT)
        row = self.generate_row(index=index)
        dt = row[4]
        sqream_cursor.execute(f"select * from {self.TEMP_TABLE_NAME} where dt = ?", params=(dt,))
        assert sqream_cursor.fetchall() == [row]

    def test_text(self, sqream_cursor):
        index = randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT)
        t = self.RANDOM_TEXT.format(index=index)
        sqream_cursor.execute(f"select * from {self.TEMP_TABLE_NAME} where t = ?", params=(t,))
        assert sqream_cursor.fetchall() == [self.generate_row(index)]

    def test_int_array(self, sqream_cursor):
        index = randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT)
        sqream_cursor.execute(f"select * from {self.TEMP_TABLE_NAME} where iar = ?::int[]",
                              params=([index] * DEFAULT_ARRAY_ELEMENTS_AMOUNT,))
        assert sqream_cursor.fetchall() == [self.generate_row(index)]

    def test_bool_array(self, sqream_cursor):
        sqream_cursor.execute(f"select * from {self.TEMP_TABLE_NAME} where bar = ?::bool[]",
                              params=([True] * DEFAULT_ARRAY_ELEMENTS_AMOUNT,))
        assert sqream_cursor.fetchall() == [self.generate_row(i) for i in range(1, 10)]

    def test_numeric_array(self, sqream_cursor):
        index = randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT)
        numeric = Decimal(self.RANDOM_NUMERIC.format(index=index))
        sqream_cursor.execute(f"select * from {self.TEMP_TABLE_NAME} where nar = ?::numeric(15,6)[]",
                              params=([numeric] * DEFAULT_ARRAY_ELEMENTS_AMOUNT,))
        assert sqream_cursor.fetchall() == [self.generate_row(index)]

    def test_date_array(self, sqream_cursor):
        index = randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT)
        row = self.generate_row(index=index)
        d = row[3]
        sqream_cursor.execute(f"select * from {self.TEMP_TABLE_NAME} where dar = ?::date[]",
                              params=([d] * DEFAULT_ARRAY_ELEMENTS_AMOUNT,))
        assert sqream_cursor.fetchall() == [row]

    def test_datetime_array(self, sqream_cursor):
        index = randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT)
        row = self.generate_row(index=index)
        dt = row[4]
        sqream_cursor.execute(f"select * from {self.TEMP_TABLE_NAME} where dtar = ?::datetime[]",
                              params=([dt] * DEFAULT_ARRAY_ELEMENTS_AMOUNT,))
        assert sqream_cursor.fetchall() == [row]

    def test_text_array(self, sqream_cursor):
        index = randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT)
        t = self.RANDOM_TEXT.format(index=index)
        sqream_cursor.execute(f"select * from {self.TEMP_TABLE_NAME} where tar = ?::text[]",
                              params=([t] * DEFAULT_ARRAY_ELEMENTS_AMOUNT,))
        assert sqream_cursor.fetchall() == [self.generate_row(index)]


class TestInsert(TestBaseParameterizedStatements):

    def test_int(self, sqream_cursor):
        index = 10
        sqream_cursor.execute(f"insert into {self.TEMP_TABLE_NAME} (i) values (?)", params=(index,))
        sqream_cursor.execute(f"select i from {self.TEMP_TABLE_NAME} where i = ?", params=(index,))
        assert sqream_cursor.fetchall() == [(10,)]

    def test_bool(self, sqream_cursor):
        sqream_cursor.execute(f"insert into {self.TEMP_TABLE_NAME} (b) values (?)", params=(True,))
        sqream_cursor.execute(f"select b from {self.TEMP_TABLE_NAME} where b = ?", params=(True,))
        assert sqream_cursor.fetchall() == [(True,)]

    def test_numeric(self, sqream_cursor):
        n = Decimal(f"{randint(int(1e8), int(1e9-1))}.{randint(int(1e5), int(1e6-1))}")
        sqream_cursor.execute(f"insert into {self.TEMP_TABLE_NAME} (n) values (?)", params=(n,))
        sqream_cursor.execute(f"select n from {self.TEMP_TABLE_NAME} where n = ?", params=(n,))
        assert sqream_cursor.fetchall() == [(n,)]

    def test_date(self, sqream_cursor):
        d = date(2024, 9, 20)
        sqream_cursor.execute(f"insert into {self.TEMP_TABLE_NAME} (d) values (?)", params=(d,))
        sqream_cursor.execute(f"select d from {self.TEMP_TABLE_NAME} where d = ?", params=(d,))
        assert sqream_cursor.fetchall() == [(d,)]

    def test_datetime(self, sqream_cursor):
        dt = datetime(2024, 9, 20, 23, 30, 25, 123000)
        sqream_cursor.execute(f"insert into {self.TEMP_TABLE_NAME} (dt) values (?)", params=(dt,))
        sqream_cursor.execute(f"select dt from {self.TEMP_TABLE_NAME} where dt = ?", params=(dt,))
        assert sqream_cursor.fetchall() == [(dt,)]

    def test_text(self, sqream_cursor):
        t = "Ryan Gosling"
        sqream_cursor.execute(f"insert into {self.TEMP_TABLE_NAME} (t) values (?)", params=(t,))
        sqream_cursor.execute(f"select t from {self.TEMP_TABLE_NAME} where t = ?", params=(t,))
        assert sqream_cursor.fetchall() == [(t,)]

    def test_int_array(self, sqream_cursor):
        index = 10
        sqream_cursor.execute(f"insert into {self.TEMP_TABLE_NAME} (iar) values (?)",
                              params=([index] * DEFAULT_ARRAY_ELEMENTS_AMOUNT,))
        sqream_cursor.execute(f"select iar from {self.TEMP_TABLE_NAME} where iar = ?::int[]",
                              params=([index] * DEFAULT_ARRAY_ELEMENTS_AMOUNT,))
        assert sqream_cursor.fetchall() == [([index] * DEFAULT_ARRAY_ELEMENTS_AMOUNT,)]

    def test_bool_array(self, sqream_cursor):
        sqream_cursor.execute(f"insert into {self.TEMP_TABLE_NAME} (bar) values (?)",
                              params=([False] * DEFAULT_ARRAY_ELEMENTS_AMOUNT,))
        sqream_cursor.execute(f"select bar from {self.TEMP_TABLE_NAME} where bar = ?::bool[]",
                              params=([False] * DEFAULT_ARRAY_ELEMENTS_AMOUNT,))
        assert sqream_cursor.fetchall() == [([False] * DEFAULT_ARRAY_ELEMENTS_AMOUNT,)]

    def test_insert_several_columns_without_arrays(self, sqream_cursor):
        params = (10, Decimal('123.123000'), date(2020, 10, 12), 'kavabanga')
        sqream_cursor.execute(f"insert into {self.TEMP_TABLE_NAME} (i, n, d, t) values "
                                f"(?, ?, ?, ?)",
                              params=params)
        sqream_cursor.execute(f"select i, n, d, t from {self.TEMP_TABLE_NAME} where i = ?",
                              params=(params[0],))
        data = sqream_cursor.fetchall()
        assert data[0] == params
        sqream_cursor.execute(f"delete from {self.TEMP_TABLE_NAME} where i = ?", params=(params[0],))


class TestUpdate(TestBaseParameterizedStatements):

    def test_int(self, sqream_cursor):
        old, new = randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT), 10
        sqream_cursor.execute(f"update {self.TEMP_TABLE_NAME} set i = ? where i = ?",
                              params=(new, old))
        sqream_cursor.execute(f"select i from {self.TEMP_TABLE_NAME} where i = ?", params=(new,))
        assert sqream_cursor.fetchall() == [(new,)]

    def test_bool(self, sqream_cursor):
        sqream_cursor.execute(f"update {self.TEMP_TABLE_NAME} set b = ? where b = ?",
                              params=(True, False))
        sqream_cursor.execute(f"select b from {self.TEMP_TABLE_NAME} where b = ?", params=(True,))
        assert sqream_cursor.fetchall() == [(True,)] * self.TEMP_TABLE_START_ROWS_AMOUNT

    def test_numeric(self, sqream_cursor):
        new = Decimal(f"{randint(int(1e8), int(1e9-1))}.{randint(int(1e5), int(1e6-1))}")
        old = Decimal(self.RANDOM_NUMERIC.format(index=randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT)))
        sqream_cursor.execute(f"update {self.TEMP_TABLE_NAME} set n = ? where n = ?",
                              params=(new, old))
        sqream_cursor.execute(f"select n from {self.TEMP_TABLE_NAME} where n = ?", params=(new,))
        assert sqream_cursor.fetchall() == [(new,)]

    def test_date(self, sqream_cursor):
        new = date(2024, 9, 20)
        old = date.fromisoformat(self.RANDOM_DATE.format(index=randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT)))
        sqream_cursor.execute(f"update {self.TEMP_TABLE_NAME} set d = ? where d = ?",
                              params=(new, old))
        sqream_cursor.execute(f"select d from {self.TEMP_TABLE_NAME} where d = ?", params=(new,))
        assert sqream_cursor.fetchall() == [(new,)]

    def test_datetime(self, sqream_cursor):
        new = datetime(2024, 9, 20, 23, 30, 25, 123000)
        old = datetime.strptime(self.RANDOM_DATETIME.format(index=randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT)),
                                "%Y-%m-%d %H:%M:%S.%f")
        sqream_cursor.execute(f"update {self.TEMP_TABLE_NAME} set dt = ? where dt = ?",
                              params=(new, old))
        sqream_cursor.execute(f"select dt from {self.TEMP_TABLE_NAME} where dt = ?", params=(new,))
        assert sqream_cursor.fetchall() == [(new,)]

    def test_text(self, sqream_cursor):
        new = "Ryan Gosling"
        old = self.RANDOM_TEXT.format(index=randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT))
        sqream_cursor.execute(f"update {self.TEMP_TABLE_NAME} set t = ? where t = ?",
                              params=(new, old))
        sqream_cursor.execute(f"select t from {self.TEMP_TABLE_NAME} where t = ?", params=(new,))
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
        sqream_cursor.execute(f"delete from {self.TEMP_TABLE_NAME} where i = ?", params=(index,))
        sqream_cursor.execute(f"select i from {self.TEMP_TABLE_NAME} where i = ?", params=(index,))
        assert sqream_cursor.fetchall() == []

    def test_bool(self, sqream_cursor, check_number_of_rows_has_decreased):
        sqream_cursor.execute(f"delete from {self.TEMP_TABLE_NAME} where b = ?", params=(False,))
        sqream_cursor.execute(f"select * from {self.TEMP_TABLE_NAME}")
        assert sqream_cursor.fetchall() == []

    def test_numeric(self, sqream_cursor, check_number_of_rows_has_decreased):
        n = Decimal(self.RANDOM_NUMERIC.format(index=randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT)))
        sqream_cursor.execute(f"delete from {self.TEMP_TABLE_NAME} where n = ?", params=(n,))
        sqream_cursor.execute(f"select n from {self.TEMP_TABLE_NAME} where n = ?", params=(n,))
        assert sqream_cursor.fetchall() == []

    def test_date(self, sqream_cursor, check_number_of_rows_has_decreased):
        d = date.fromisoformat(self.RANDOM_DATE.format(index=1))
        sqream_cursor.execute(f"delete from {self.TEMP_TABLE_NAME} where d = ?", params=(d,))
        sqream_cursor.execute(f"select d from {self.TEMP_TABLE_NAME} where d = ?", params=(d,))
        assert sqream_cursor.fetchall() == []

    def test_datetime(self, sqream_cursor, check_number_of_rows_has_decreased):
        dt = datetime.strptime(TestBaseParameterizedStatements.RANDOM_DATETIME.format(index=1), "%Y-%m-%d %H:%M:%S.%f")
        sqream_cursor.execute(f"delete from {self.TEMP_TABLE_NAME} where dt = ?", params=(dt,))
        sqream_cursor.execute(f"select dt from {self.TEMP_TABLE_NAME} where dt = ?", params=(dt,))
        assert sqream_cursor.fetchall() == []

    def test_text(self, sqream_cursor, check_number_of_rows_has_decreased):
        t = self.RANDOM_TEXT.format(index=randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT))
        sqream_cursor.execute(f"delete from {self.TEMP_TABLE_NAME} where t = ?", params=(t,))
        sqream_cursor.execute(f"select t from {self.TEMP_TABLE_NAME} where t = ?", params=(t,))
        assert sqream_cursor.fetchall() == []


class TestNegative(TestBaseParameterizedStatements):

    def test_select_placeholder_params_differ_amount(self, sqream_cursor):
        with pytest.raises(Exception) as error:
            sqream_cursor.execute(f"select * from {self.TEMP_TABLE_NAME} where i in (?, ?, ?)", params=(1, 2, 3, 4))

        expected_error = "Incosistent data sequences passed for inserting. Please use rows/columns of consistent length"
        assert expected_error in str(error), f"Can not find expected error `{expected_error}` in real one `{error}`"

    def test_select_placeholders_in_wrong_places(self, sqream_cursor):
        with pytest.raises(Exception) as parameterized_error:
            sqream_cursor.execute(f"select * from ? where i = ?", params=(1, 2))
        assert "Parsing error in statement line" in str(parameterized_error)
