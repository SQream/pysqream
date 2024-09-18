from datetime import date
from decimal import Decimal

import pytest

from pysqream.utils import ParametrizedStatementError
from tests.test_base import TestBaseParametrizedStatements


class TestSelect(TestBaseParametrizedStatements):
    params = tuple(range(1, TestBaseParametrizedStatements.TEMP_TABLE_START_ROWS_AMOUNT + 1))
    expected_result = [(i, False) for i in params]

    @pytest.mark.parametrize("param", params)
    def test_less_one_qmark(self, sqream_cursor, param: int):
        sqream_cursor.execute(f"select i, b from {self.TEMP_TABLE_NAME} where i < ?", params=(param,))
        assert sqream_cursor.fetchall() == self.expected_result[:param - 1]

    @pytest.mark.parametrize("param", params)
    def test_greater_one_qmark(self, sqream_cursor, param: int):
        sqream_cursor.execute(f"select i, b from {self.TEMP_TABLE_NAME} where i > ?", params=(param,))
        assert sqream_cursor.fetchall() == self.expected_result[param:]

    @pytest.mark.parametrize("param", params)
    def test_less_equal_one_qmark(self, sqream_cursor, param: int):
        sqream_cursor.execute(f"select i, b from {self.TEMP_TABLE_NAME} where i <= ?", params=(param,))
        assert sqream_cursor.fetchall() == self.expected_result[:param]

    @pytest.mark.parametrize("param", params)
    def test_greater_equal_one_qmark(self, sqream_cursor, param: int):
        sqream_cursor.execute(f"select i, b from {self.TEMP_TABLE_NAME} where i >= ?", params=(param,))
        assert sqream_cursor.fetchall() == self.expected_result[param - 1:]

    @pytest.mark.parametrize("param", params)
    def test_equal_one_qmark(self, sqream_cursor, param: int):
        sqream_cursor.execute(f"select i, b from {self.TEMP_TABLE_NAME} where i = ?", params=(param,))
        assert sqream_cursor.fetchall() == [self.expected_result[param - 1]]

    def test_like_one_qmark(self, sqream_cursor):
        sqream_cursor.execute(f"select t from {self.TEMP_TABLE_NAME} where t like ?", params=('%text',))
        assert sqream_cursor.fetchall() == [(f"{i} text",) for i in range(1, self.TEMP_TABLE_START_ROWS_AMOUNT + 1)]

        sqream_cursor.execute(f"select t, tar from {self.TEMP_TABLE_NAME} where tar[1] like ?", params=('1%',))
        assert sqream_cursor.fetchall() == [("1 text", ["1 text", "1 text", "1 text"])]

    def test_in_one_qmark(self, sqream_cursor):
        sqream_cursor.execute(f"select i, b from {self.TEMP_TABLE_NAME} where i in (?)", params=(1,))
        assert sqream_cursor.fetchall() == [(1, False)]


class TestInsert(TestBaseParametrizedStatements):

    def test_insert(self, sqream_cursor):
        params = (10, Decimal('123.123000'), date(2020, 10, 12), 'kavabanga')
        sqream_cursor.execute(f"insert into {self.TEMP_TABLE_NAME} (i, n, d, t) values (?, ?, ?, ?)", params=params)
        sqream_cursor.execute(f"select i, n, d, t from {self.TEMP_TABLE_NAME} where i = ?", params=(params[0],))
        data = sqream_cursor.fetchall()
        assert data[0] == params
        sqream_cursor.execute(f"delete from {self.TEMP_TABLE_NAME} where i = ?", params=(params[0],))


class TestUpdate(TestBaseParametrizedStatements):

    def test_update_one_criteria(self, sqream_cursor):
        params = (10, 1)
        sqream_cursor.execute(f"update {self.TEMP_TABLE_NAME} set i = ? where i = ?", params=params)

        sqream_cursor.execute(f"select i from {self.TEMP_TABLE_NAME} where i = ?", params=(params[0],))
        data = sqream_cursor.fetchall()
        assert data[0] == (params[0],)


class TestDelete(TestBaseParametrizedStatements):

    def test_delete_one_parameter(self, sqream_cursor):
        # Insert 1 row and make sure it was inserted successfully by selecting it
        params = (10,)
        sqream_cursor.execute(f"insert into {self.TEMP_TABLE_NAME} (i) values (?)", params=params)

        sqream_cursor.execute(f"select i from {self.TEMP_TABLE_NAME} where i = ?", params=params)
        data = sqream_cursor.fetchone()
        assert data[0] == params[0]

        # Delete this row and make sure it was removed successfully by getting empty queryset
        sqream_cursor.execute(f"delete from {self.TEMP_TABLE_NAME} where i = ?", params=params)

        sqream_cursor.execute(f"select i from {self.TEMP_TABLE_NAME} where i = ?", params=params)
        data = sqream_cursor.fetchall()
        assert data == []


class TestNegative(TestBaseParametrizedStatements):

    @pytest.mark.parametrize(
        ("condition", "params"),
        (
                ("i > 5", (1,)),
                ("i > ? and b = ? or d < ?", (1, 2)),
                ("i in (1, 2, 3)", (1, 2, 3, 4)),
        ),
        ids=("no_placeholder", "more_placeholders_than_params", "less_placeholders_than_params")
    )
    def test_select_placeholder_params_differ_amount(self, sqream_cursor, condition, params):
        with pytest.raises(ParametrizedStatementError) as error:
            sqream_cursor.execute(f"select * from {self.TEMP_TABLE_NAME} where {condition}", params=params)

        expected_error = (f"Amount of parameters ({len(params)}) doesn't equal "
                          f"amount of placeholders ({condition.count('?')})")
        assert expected_error in str(error), f"Can not find expected error `{expected_error}` in real `{error}`"

    def test_select_placeholders_in_wrong_places(self, sqream_cursor):
        with pytest.raises(ParametrizedStatementError) as parametrized_error:
            sqream_cursor.execute(f"select * from ? where i = ?", params=(1, 2))
        assert f"Amount of parameters (2) doesn't equal amount of placeholders (1)" in str(parametrized_error)
