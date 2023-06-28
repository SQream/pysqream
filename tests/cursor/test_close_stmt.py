"""Tests for catching errors on closeStatement

In the past, the closeStatement operation remained silent, causing interference
with SQ-13979. As a result, when inserting sequences (TEXT / ARRAY) that
exceeded the limited size, the process would silently close, giving the
impression of success.
"""
import pytest

from pysqream.errors import OperationalError, ProgrammingError


def _patch_send(monkeypatch, mock_cursor, response):
    monkeypatch.setattr(mock_cursor.client, "send_string", lambda *_: response)


def test_raise_on_error_from_sqream(monkeypatch, mock_cursor):
    """JSON with error from SQREAM on closeStatement raises OperationalError"""
    _patch_send(monkeypatch, mock_cursor, '{"error": "mock SQREAM error"}')
    mock_cursor.open_statement = True
    with pytest.raises(OperationalError):
        mock_cursor.close_stmt()


def test_raise_on_invalid_json(monkeypatch, mock_cursor):
    """
    Test if SQREAM sends invalid json on closeStatement raises ProgrammingError
    """
    _patch_send(monkeypatch, mock_cursor, "I'm invalid json")
    mock_cursor.open_statement = True
    with pytest.raises(ProgrammingError):
        mock_cursor.close_stmt()


def test_raise_if_json_not_obj(monkeypatch, mock_cursor):
    """
    Test if SQREAM sends not object on closeStatement raises ProgrammingError
    """
    _patch_send(monkeypatch, mock_cursor, '["valid array"]')
    mock_cursor.open_statement = True
    with pytest.raises(ProgrammingError):
        mock_cursor.close_stmt()
