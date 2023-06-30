"""Utility for testing array fetch and insert"""
from datetime import date, datetime
from decimal import Decimal
from typing import Any, List


# Hide traceback for pylint, so it show only function at place it called
__tracebackhide__ = True  # pylint: disable=unused-variable

ALL_TYPES = [
    "BOOL",
    "TINYINT",
    "SMALLINT",
    "INT",
    "BIGINT",
    "REAL",
    "DOUBLE",
    "NUMERIC(38,38)",
    "NUMERIC(12,4)",
    "DATE",
    "DATETIME",
    "TEXT",
]

SIMPLE_VALUES = [
    [True, False],
    [1, 255],
    [256, 32767],
    [32768, 2147483647],
    [2147483648, 9223372036854775807],

    [3.1410000324249268, 5.315000057220459],
    [0.000003, 101.000026],
    [Decimal('0.12324567890123456789012345678901234567')],
    [Decimal('131235.1232')],

    [date(1955, 11, 5), date(9999, 12, 31)],
    [
        datetime(1955, 11, 5, 1, 24),
        # Does not work unless SQ-13967 & SQ-13969 fixed
        # datetime(9999, 12, 31, 23, 59, 59, 999),
    ],

    ["Kiwis have tiny wings, but cannot fly.", ""],
]


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
