"""Tests for catching errors on closeStatement

In the past, the closeStatement operation remained silent, causing interference
with SQ-13979. As a result, when inserting sequences (TEXT / ARRAY) that
exceeded the limited size, the process would silently close, giving the
impression of success.
"""
import pytest

from pysqream.cursor import Cursor
from pysqream.SQSocket import Client
from pysqream.utils import OperationalError, ProgrammingError


class ConnectionMock:
    """Mock of pysqream.connection.Connection to prevent real connection"""
    # pylint: disable=too-few-public-methods; they are not need for Mocks
    socket = None
    client = Client
    ping_loop = None
    connection_id = None
    version = 'Mock1'


def test_raise_on_error_from_sqream(monkeypatch):
    """JSON with error from SQREAM on closeStatement raises OperationalError"""
    monkeypatch.setattr(Client, "send_string", lambda *_: '{"error": "mock SQREAM error"}')
    cur = Cursor(ConnectionMock(), [])
    cur.open_statement = True
    with pytest.raises(OperationalError):
        cur.close_stmt()


def test_raise_on_invalid_json(monkeypatch):
    """
    Test if SQREAM sends invalid json on closeStatement raises ProgrammingError
    """
    monkeypatch.setattr(Client, "send_string", lambda *_: "I'm invalid json")
    cur = Cursor(ConnectionMock(), [])
    cur.open_statement = True
    with pytest.raises(ProgrammingError):
        cur.close_stmt()


def test_raise_if_json_not_obj(monkeypatch):
    """
    Test if SQREAM sends not object on closeStatement raises ProgrammingError
    """
    monkeypatch.setattr(Client, "send_string", lambda *_: '["valid array"]')
    cur = Cursor(ConnectionMock(), [])
    cur.open_statement = True
    with pytest.raises(ProgrammingError):
        cur.close_stmt()
