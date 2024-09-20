from datetime import date, datetime
from decimal import Decimal
from random import randint

import pytest

from pysqream.utils import ParametrizedStatementError
from tests.test_base import TestBaseParametrizedStatements


@pytest.mark.parametrize("placeholder", ("?", "%s"), ids=("qmark", "percent_s"))
class TestSelect(TestBaseParametrizedStatements):
    params = (1, 5, TestBaseParametrizedStatements.TEMP_TABLE_START_ROWS_AMOUNT)
    expected_result = [(i, False) for i in range(1, TestBaseParametrizedStatements.TEMP_TABLE_START_ROWS_AMOUNT + 1)]

    @pytest.mark.parametrize("param", params)
    def test_less_one_placeholder(self, sqream_cursor, param: int, placeholder):
        sqream_cursor.execute(f"select i, b from {self.TEMP_TABLE_NAME} where i < {placeholder}", params=(param,))
        assert sqream_cursor.fetchall() == self.expected_result[:param - 1]

    @pytest.mark.parametrize("param", params)
    def test_greater_one_placeholder(self, sqream_cursor, param: int, placeholder):
        sqream_cursor.execute(f"select i, b from {self.TEMP_TABLE_NAME} where i > {placeholder}", params=(param,))
        assert sqream_cursor.fetchall() == self.expected_result[param:]

    @pytest.mark.parametrize("param", params)
    def test_less_equal_one_placeholder(self, sqream_cursor, param: int, placeholder):
        sqream_cursor.execute(f"select i, b from {self.TEMP_TABLE_NAME} where i <= {placeholder}", params=(param,))
        assert sqream_cursor.fetchall() == self.expected_result[:param]

    @pytest.mark.parametrize("param", params)
    def test_greater_equal_one_placeholder(self, sqream_cursor, param: int, placeholder):
        sqream_cursor.execute(f"select i, b from {self.TEMP_TABLE_NAME} where i >= {placeholder}", params=(param,))
        assert sqream_cursor.fetchall() == self.expected_result[param - 1:]

    @pytest.mark.parametrize("param", params)
    def test_equal_one_placeholder(self, sqream_cursor, param: int, placeholder):
        sqream_cursor.execute(f"select i, b from {self.TEMP_TABLE_NAME} where i = {placeholder}", params=(param,))
        assert sqream_cursor.fetchall() == [self.expected_result[param - 1]]

    def test_like_one_placeholder(self, sqream_cursor, placeholder):
        sqream_cursor.execute(f"select t from {self.TEMP_TABLE_NAME} where t like {placeholder}", params=('%text',))
        assert sqream_cursor.fetchall() == [(f"{i} text",) for i in range(1, self.TEMP_TABLE_START_ROWS_AMOUNT + 1)]

        sqream_cursor.execute(f"select t, tar from {self.TEMP_TABLE_NAME} where tar[1] like {placeholder}",
                              params=('1%',))
        assert sqream_cursor.fetchall() == [("1 text", ["1 text", "1 text", "1 text"])]

    def test_in_one_placeholder(self, sqream_cursor, placeholder):
        sqream_cursor.execute(f"select i, b from {self.TEMP_TABLE_NAME} where i in ({placeholder})", params=(1,))
        assert sqream_cursor.fetchall() == [(1, False)]

    def test_int(self, sqream_cursor, placeholder):
        index = randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT)
        sqream_cursor.execute(f"select * from {self.TEMP_TABLE_NAME} where i = {placeholder}", params=(index,))
        assert sqream_cursor.fetchall() == [self.generate_row(index)]

    def test_bool(self, sqream_cursor, placeholder):
        sqream_cursor.execute(f"select * from {self.TEMP_TABLE_NAME} where b = {placeholder}", params=(False,))
        assert sqream_cursor.fetchall() == [self.generate_row(i) for i in range(1, 10)]

    def test_numeric(self, sqream_cursor, placeholder):
        index = randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT)
        numeric = Decimal(self.RANDOM_NUMERIC.format(index=index))
        sqream_cursor.execute(f"select * from {self.TEMP_TABLE_NAME} where n = {placeholder}", params=(numeric,))
        assert sqream_cursor.fetchall() == [self.generate_row(index)]

    def test_date_as_string(self, sqream_cursor, placeholder):
        index = randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT)
        d = self.RANDOM_DATE.format(index=index)
        sqream_cursor.execute(f"select * from {self.TEMP_TABLE_NAME} where d = {placeholder}", params=(d,))
        assert sqream_cursor.fetchall() == [self.generate_row(index)]

    def test_date_as_object(self, sqream_cursor, placeholder):
        index = randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT)
        row = self.generate_row(index=index)
        d = row[3]
        sqream_cursor.execute(f"select * from {self.TEMP_TABLE_NAME} where d = {placeholder}", params=(d,))
        assert sqream_cursor.fetchall() == [row]

    def test_datetime_as_string(self, sqream_cursor, placeholder):
        index = randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT)
        dt = self.RANDOM_DATETIME.format(index=index)
        sqream_cursor.execute(f"select * from {self.TEMP_TABLE_NAME} where dt = {placeholder}", params=(dt,))
        assert sqream_cursor.fetchall() == [self.generate_row(index)]

    def test_datetime_as_object(self, sqream_cursor, placeholder):
        index = randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT)
        row = self.generate_row(index=index)
        dt = row[4]
        sqream_cursor.execute(f"select * from {self.TEMP_TABLE_NAME} where dt = {placeholder}", params=(dt,))
        assert sqream_cursor.fetchall() == [row]

    def test_text(self, sqream_cursor, placeholder):
        index = randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT)
        t = self.RANDOM_TEXT.format(index=index)
        sqream_cursor.execute(f"select * from {self.TEMP_TABLE_NAME} where t = {placeholder}", params=(t,))
        assert sqream_cursor.fetchall() == [self.generate_row(index)]


@pytest.mark.parametrize("placeholder", ("?", "%s"), ids=("qmark", "percent_s"))
class TestInsert(TestBaseParametrizedStatements):

    def test_int(self, sqream_cursor, placeholder):
        index = 10
        sqream_cursor.execute(f"insert into {self.TEMP_TABLE_NAME} (i) values ({placeholder})", params=(index,))
        sqream_cursor.execute(f"select i from {self.TEMP_TABLE_NAME} where i = {placeholder}", params=(index,))
        assert sqream_cursor.fetchall() == [(10,)]

    def test_bool(self, sqream_cursor, placeholder):
        sqream_cursor.execute(f"insert into {self.TEMP_TABLE_NAME} (b) values ({placeholder})", params=(True,))
        sqream_cursor.execute(f"select b from {self.TEMP_TABLE_NAME} where b = {placeholder}", params=(True,))
        assert sqream_cursor.fetchall() == [(True,)]

    def test_numeric(self, sqream_cursor, placeholder):
        n = Decimal(f"{randint(int(1e8), int(1e9-1))}.{randint(int(1e5), int(1e6-1))}")
        sqream_cursor.execute(f"insert into {self.TEMP_TABLE_NAME} (n) values ({placeholder})", params=(n,))
        sqream_cursor.execute(f"select n from {self.TEMP_TABLE_NAME} where n = {placeholder}", params=(n,))
        assert sqream_cursor.fetchall() == [(n,)]

    def test_date_as_string(self, sqream_cursor, placeholder):
        d = date(2024, 9, 20)
        sqream_cursor.execute(f"insert into {self.TEMP_TABLE_NAME} (d) values ({placeholder})", params=(d.strftime("%F"),))
        sqream_cursor.execute(f"select d from {self.TEMP_TABLE_NAME} where d = {placeholder}", params=(d.strftime("%F"),))
        assert sqream_cursor.fetchall() == [(d,)]

    def test_date_as_object(self, sqream_cursor, placeholder):
        d = date(2024, 9, 20)
        sqream_cursor.execute(f"insert into {self.TEMP_TABLE_NAME} (d) values ({placeholder})", params=(d,))
        sqream_cursor.execute(f"select d from {self.TEMP_TABLE_NAME} where d = {placeholder}", params=(d,))
        assert sqream_cursor.fetchall() == [(d,)]

    def test_datetime_as_string(self, sqream_cursor, placeholder):
        dt = datetime(2024, 9, 20, 23, 30, 25, 123000)
        sqream_cursor.execute(f"insert into {self.TEMP_TABLE_NAME} (dt) values ({placeholder})", params=(dt.strftime("%F %T.%f"),))
        sqream_cursor.execute(f"select dt from {self.TEMP_TABLE_NAME} where dt = {placeholder}", params=(dt.strftime("%F %T.%f"),))
        assert sqream_cursor.fetchall() == [(dt,)]

    def test_datetime_as_object(self, sqream_cursor, placeholder):
        dt = datetime(2024, 9, 20, 23, 30, 25, 123000)
        sqream_cursor.execute(f"insert into {self.TEMP_TABLE_NAME} (dt) values ({placeholder})", params=(dt,))
        sqream_cursor.execute(f"select dt from {self.TEMP_TABLE_NAME} where dt = {placeholder}", params=(dt,))
        assert sqream_cursor.fetchall() == [(dt,)]

    def test_text(self, sqream_cursor, placeholder):
        t = "Ryan Gosling"
        sqream_cursor.execute(f"insert into {self.TEMP_TABLE_NAME} (t) values ({placeholder})", params=(t,))
        sqream_cursor.execute(f"select t from {self.TEMP_TABLE_NAME} where t = {placeholder}", params=(t,))
        assert sqream_cursor.fetchall() == [(t,)]

    def test_insert_several_columns_without_arrays(self, sqream_cursor, placeholder):
        params = (10, Decimal('123.123000'), date(2020, 10, 12), 'kavabanga')
        sqream_cursor.execute(f"insert into {self.TEMP_TABLE_NAME} (i, n, d, t) values "
                                f"({placeholder}, {placeholder}, {placeholder}, {placeholder})",
                              params=params)
        sqream_cursor.execute(f"select i, n, d, t from {self.TEMP_TABLE_NAME} where i = {placeholder}",
                              params=(params[0],))
        data = sqream_cursor.fetchall()
        assert data[0] == params
        sqream_cursor.execute(f"delete from {self.TEMP_TABLE_NAME} where i = {placeholder}", params=(params[0],))


@pytest.mark.parametrize("placeholder", ("?", "%s"), ids=("qmark", "percent_s"))
class TestUpdate(TestBaseParametrizedStatements):

    def test_int(self, sqream_cursor, placeholder):
        old, new = randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT), 10
        sqream_cursor.execute(f"update {self.TEMP_TABLE_NAME} set i = {placeholder} where i = {placeholder}",
                              params=(new, old))
        sqream_cursor.execute(f"select i from {self.TEMP_TABLE_NAME} where i = {placeholder}", params=(new,))
        assert sqream_cursor.fetchall() == [(new,)]

    def test_bool(self, sqream_cursor, placeholder):
        sqream_cursor.execute(f"update {self.TEMP_TABLE_NAME} set b = {placeholder} where b = {placeholder}",
                              params=(True, False))
        sqream_cursor.execute(f"select b from {self.TEMP_TABLE_NAME} where b = {placeholder}", params=(True,))
        assert sqream_cursor.fetchall() == [(True,)] * self.TEMP_TABLE_START_ROWS_AMOUNT

    def test_numeric(self, sqream_cursor, placeholder):
        new = Decimal(f"{randint(int(1e8), int(1e9-1))}.{randint(int(1e5), int(1e6-1))}")
        old = Decimal(self.RANDOM_NUMERIC.format(index=randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT)))
        sqream_cursor.execute(f"update {self.TEMP_TABLE_NAME} set n = {placeholder} where n = {placeholder}",
                              params=(new, old))
        sqream_cursor.execute(f"select n from {self.TEMP_TABLE_NAME} where n = {placeholder}", params=(new,))
        assert sqream_cursor.fetchall() == [(new,)]

    def test_date_as_string(self, sqream_cursor, placeholder):
        new = date(2024, 9, 20)
        old = date.fromisoformat(self.RANDOM_DATE.format(index=randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT)))
        sqream_cursor.execute(f"update {self.TEMP_TABLE_NAME} set d = {placeholder} where d = {placeholder}",
                              params=(new.strftime("%F"), old.strftime("%F")))
        sqream_cursor.execute(f"select d from {self.TEMP_TABLE_NAME} where d = {placeholder}",
                              params=(new.strftime("%F"),))
        assert sqream_cursor.fetchall() == [(new,)]

    def test_date_as_object(self, sqream_cursor, placeholder):
        new = date(2024, 9, 20)
        old = date.fromisoformat(self.RANDOM_DATE.format(index=randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT)))
        sqream_cursor.execute(f"update {self.TEMP_TABLE_NAME} set d = {placeholder} where d = {placeholder}",
                              params=(new, old))
        sqream_cursor.execute(f"select d from {self.TEMP_TABLE_NAME} where d = {placeholder}", params=(new,))
        assert sqream_cursor.fetchall() == [(new,)]

    def test_datetime_as_string(self, sqream_cursor, placeholder):
        new = datetime(2024, 9, 20, 23, 30, 25, 123000)
        old = datetime.strptime(self.RANDOM_DATETIME.format(index=randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT)),
                                "%Y-%m-%d %H:%M:%S.%f")
        sqream_cursor.execute(f"update {self.TEMP_TABLE_NAME} set dt = {placeholder} where dt = {placeholder}",
                              params=(new.strftime("%F %T.%f"), old.strftime("%F %T.%f")))
        sqream_cursor.execute(f"select dt from {self.TEMP_TABLE_NAME} where dt = {placeholder}",
                              params=(new.strftime("%F %T.%f"),))
        assert sqream_cursor.fetchall() == [(new,)]

    def test_datetime_as_object(self, sqream_cursor, placeholder):
        new = datetime(2024, 9, 20, 23, 30, 25, 123000)
        old = datetime.strptime(self.RANDOM_DATETIME.format(index=randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT)),
                                "%Y-%m-%d %H:%M:%S.%f")
        sqream_cursor.execute(f"update {self.TEMP_TABLE_NAME} set dt = {placeholder} where dt = {placeholder}",
                              params=(new, old))
        sqream_cursor.execute(f"select dt from {self.TEMP_TABLE_NAME} where dt = {placeholder}", params=(new,))
        assert sqream_cursor.fetchall() == [(new,)]

    def test_text(self, sqream_cursor, placeholder):
        new = "Ryan Gosling"
        old = self.RANDOM_TEXT.format(index=randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT))
        sqream_cursor.execute(f"update {self.TEMP_TABLE_NAME} set t = {placeholder} where t = {placeholder}",
                              params=(new, old))
        sqream_cursor.execute(f"select t from {self.TEMP_TABLE_NAME} where t = {placeholder}", params=(new,))
        assert sqream_cursor.fetchall() == [(new,)]


@pytest.mark.parametrize("placeholder", ("?", "%s"), ids=("qmark", "percent_s"))
class TestDelete(TestBaseParametrizedStatements):

    @pytest.fixture
    def check_number_of_rows_has_decreased(self, sqream_cursor):
        sqream_cursor.execute(f"select count(*) from {self.TEMP_TABLE_NAME}")
        rows_amount_before = sqream_cursor.fetchone()[0]
        yield
        sqream_cursor.execute(f"select count(*) from {self.TEMP_TABLE_NAME}")
        rows_amount_after = sqream_cursor.fetchone()[0]
        assert rows_amount_before > rows_amount_after, (f"Rows amount before must be greater than "
                                                        f"after: `{rows_amount_before}` !> `{rows_amount_after}`")

    def test_int(self, sqream_cursor, check_number_of_rows_has_decreased, placeholder):
        index = randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT)
        sqream_cursor.execute(f"delete from {self.TEMP_TABLE_NAME} where i = {placeholder}", params=(index,))
        sqream_cursor.execute(f"select i from {self.TEMP_TABLE_NAME} where i = {placeholder}", params=(index,))
        assert sqream_cursor.fetchall() == []

    def test_bool(self, sqream_cursor, check_number_of_rows_has_decreased, placeholder):
        sqream_cursor.execute(f"delete from {self.TEMP_TABLE_NAME} where b = {placeholder}", params=(False,))
        sqream_cursor.execute(f"select * from {self.TEMP_TABLE_NAME}")
        assert sqream_cursor.fetchall() == []

    def test_numeric(self, sqream_cursor, check_number_of_rows_has_decreased, placeholder):
        n = Decimal(self.RANDOM_NUMERIC.format(index=randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT)))
        sqream_cursor.execute(f"delete from {self.TEMP_TABLE_NAME} where n = {placeholder}", params=(n,))
        sqream_cursor.execute(f"select n from {self.TEMP_TABLE_NAME} where i = {placeholder}", params=(n,))
        assert sqream_cursor.fetchall() == []

    @pytest.mark.parametrize(
        "py_date",
        (TestBaseParametrizedStatements.RANDOM_DATE.format(index=1),
         date.fromisoformat(TestBaseParametrizedStatements.RANDOM_DATE.format(index=1))),
        ids=("as_string", "as_object")
    )
    def test_date(self, py_date, sqream_cursor, check_number_of_rows_has_decreased, placeholder):
        sqream_cursor.execute(f"delete from {self.TEMP_TABLE_NAME} where d = {placeholder}", params=(py_date,))
        sqream_cursor.execute(f"select d from {self.TEMP_TABLE_NAME} where d = {placeholder}", params=(py_date,))
        assert sqream_cursor.fetchall() == []

    @pytest.mark.parametrize(
        "py_datetime",
        (TestBaseParametrizedStatements.RANDOM_DATETIME.format(index=1),
         datetime.strptime(TestBaseParametrizedStatements.RANDOM_DATETIME.format(index=1), "%Y-%m-%d %H:%M:%S.%f")),
        ids=("as_string", "as_object")
    )
    def test_datetime(self, py_datetime, sqream_cursor, check_number_of_rows_has_decreased, placeholder):
        sqream_cursor.execute(f"delete from {self.TEMP_TABLE_NAME} where dt = {placeholder}", params=(py_datetime,))
        sqream_cursor.execute(f"select dt from {self.TEMP_TABLE_NAME} where dt = {placeholder}", params=(py_datetime,))
        assert sqream_cursor.fetchall() == []

    def test_text(self, sqream_cursor, check_number_of_rows_has_decreased, placeholder):
        t = self.RANDOM_TEXT.format(index=randint(1, self.TEMP_TABLE_START_ROWS_AMOUNT))
        sqream_cursor.execute(f"delete from {self.TEMP_TABLE_NAME} where t = {placeholder}", params=(t,))
        sqream_cursor.execute(f"select t from {self.TEMP_TABLE_NAME} where t = {placeholder}", params=(t,))
        assert sqream_cursor.fetchall() == []


class TestNegative(TestBaseParametrizedStatements):

    def test_no_placeholders(self, sqream_cursor):
        query = f"select * from {self.TEMP_TABLE_NAME} where i > 5"
        with pytest.raises(ParametrizedStatementError) as error:
            sqream_cursor.execute(query, params=(5,))

        assert "No placeholders `['\\\\?', '%s']` in the statement " + f"`{query}`" == str(error.value)

    @pytest.mark.parametrize(
        ("condition", "params"),
        (
                ("i > ? and b = ? or d < ?", (1, 2)),
                ("i in (?, ?, ?)", (1, 2, 3, 4)),
        ),
        ids=("more_placeholders_than_params", "less_placeholders_than_params")
    )
    def test_select_placeholder_params_differ_amount(self, sqream_cursor, condition, params):
        with pytest.raises(ParametrizedStatementError) as error:
            sqream_cursor.execute(f"select * from {self.TEMP_TABLE_NAME} where {condition}", params=params)

        expected_error = (f"Amount of parameters ({len(params)}) doesn't equal "
                          f"amount of placeholders ({condition.count('?')})")
        assert expected_error in str(error), f"Can not find expected error `{expected_error}` in real one `{error}`"

    def test_select_placeholders_in_wrong_places(self, sqream_cursor):
        with pytest.raises(ParametrizedStatementError) as parametrized_error:
            sqream_cursor.execute(f"select * from ? where i = ?", params=(1, 2))
        assert f"Amount of parameters (2) doesn't equal amount of placeholders (1)" in str(parametrized_error)
