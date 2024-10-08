"""Test arrays disabled by default and raises on fetch and network insert"""
import pytest
from pysqream import connect

from tests.test_cursor.utils import ALL_TYPES, SIMPLE_VALUES, select


TEMP_TABLE = "test_arrays_allowed_temp"


@pytest.mark.parametrize("data_type", ALL_TYPES)
def test_array_enabled_false_default_works_on_select(sqream_cursor, data_type):
    """Test select doesn't fail from array columns with allow_array=False"""
    # Do not use ensure_empty_table because it calls select, but create
    # should not fall
    sqream_cursor.execute(f"CREATE OR REPLACE TABLE {TEMP_TABLE} (x {data_type}[])")
    assert select(sqream_cursor, TEMP_TABLE) == []


@pytest.mark.parametrize("data_type, data", zip(ALL_TYPES, SIMPLE_VALUES))
def test_array_enabled_false_default_works_on_network_insert(sqream_cursor, data_type, data):
    """Test network insert doesn't fail from array columns with allow_array=False"""
    sqream_cursor.execute(f"CREATE OR REPLACE TABLE {TEMP_TABLE} (x {data_type}[])")

    sqream_cursor.executemany(f"insert into {TEMP_TABLE} values (?)", [(data,)])
    assert select(sqream_cursor, TEMP_TABLE) == [(data,)]


@pytest.mark.parametrize('flag', [False, True])
def test_cursor_connection_get_the_same_allow_array(ip, port, flag, conn):
    """Test that allow_array flag is passed to new connection of cursor"""
    # ip is a legacy name that doesn't conform naming style so:
    # pylint: disable=invalid-name
    conn = connect(ip, port, 'master', 'sqream', 'sqream', allow_array=flag)
    assert conn.allow_array is flag

    cur = conn.cursor()
    assert cur.conn.allow_array is flag
    # cleanup
    cur.close()
    conn.close()
