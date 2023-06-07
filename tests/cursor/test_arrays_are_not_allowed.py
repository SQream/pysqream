"""Test arrays disabled by default and raises on fetch and network insert"""
import pytest
from pysqream.utils import ArraysAreDisabled

from .utils import ALL_TYPES, SIMPLE_VALUES, select


TEMP_TABLE = "test_arrays_allowed_temp"


@pytest.mark.parametrize("data_type", ALL_TYPES)
def test_array_enabled_false_default_raises_on_select(cursor, data_type):
    """Test select fails from array columns with allow_array=False"""
    # Do not use ensure_empty_table because it calls select, but create
    # should not fall
    cursor.execute(f"CREATE OR REPLACE TABLE {TEMP_TABLE} (x {data_type}[])")

    with pytest.raises(ArraysAreDisabled):
        select(cursor, TEMP_TABLE)


@pytest.mark.parametrize("data_type, data", zip(ALL_TYPES, SIMPLE_VALUES))
def test_array_enabled_false_default_raises_on_network_insert(
        cursor, data_type, data):
    """Test network insert fails from array columns with allow_array=False"""
    cursor.execute(f"CREATE OR REPLACE TABLE {TEMP_TABLE} (x {data_type}[])")

    with pytest.raises(ArraysAreDisabled):
        cursor.executemany(f"insert into {TEMP_TABLE} values (?)", [(data,)])
