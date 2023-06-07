"""Utility for testing array fetch and insert"""


def assert_table_empty(cursor, table: str):
    """Utility to check that table is empty"""
    cursor.execute(f"SELECT * FROM {table};")
    preresult = cursor.fetchall()
    assert preresult == []
