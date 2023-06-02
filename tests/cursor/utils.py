"""Utility for testing array fetch and insert"""
from typing import Any, List


def assert_table_empty(cursor, table: str) -> None:
    """Utility to check that table is empty"""
    cursor.execute(f"SELECT * FROM {table};")
    preresult = cursor.fetchall()
    assert preresult == []


def select(cursor, table: str, values='*') -> List[Any]:
    """Utility to fetch all data"""
    cursor.execute(f"SELECT {values} FROM {table};")
    return cursor.fetchall()


def ensure_empty_table(cursor, table: str, columns: str) -> None:
    """Utility to create empty table"""
    cursor.execute(f"CREATE OR REPLACE TABLE {table} ({columns})")
    assert_table_empty(cursor, table)
