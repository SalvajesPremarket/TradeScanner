from __future__ import annotations

import sqlite3

from tradescanner.db import Connection, connect


class FakeCursor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object]] = []

    def execute(self, sql, params=()):
        self.calls.append((sql, params))
        return self

    def executemany(self, sql, params):
        self.calls.append((sql, params))
        return self


class FakePostgresConnection:
    def __init__(self) -> None:
        self.cursor_instance = FakeCursor()
        self.closed = False
        self.entered = False
        self.exited = None

    def __enter__(self):
        self.entered = True
        return self

    def __exit__(self, exc_type, exc, tb):
        self.exited = (exc_type, exc, tb)
        return False

    def cursor(self):
        return self.cursor_instance

    def close(self):
        self.closed = True


def test_sqlite_connection_commits_and_closes(tmp_path):
    path = tmp_path / "scanner.sqlite3"
    with connect(path) as connection:
        connection.execute("CREATE TABLE sample (value TEXT)")
        connection.execute("INSERT INTO sample(value) VALUES (?)", ("persisted",))

    with connect(path) as connection:
        row = connection.execute("SELECT value FROM sample").fetchone()

    assert row == ("persisted",)


def test_postgres_adapter_translates_placeholders_and_closes():
    raw = FakePostgresConnection()
    connection = Connection(raw, postgres=True)

    with connection as db:
        db.execute("SELECT value FROM sample WHERE id = ?", (7,))
        db.executemany(
            "INSERT INTO sample(value) VALUES (?)",
            [("one",), ("two",)],
        )

    assert raw.entered is True
    assert raw.closed is True
    assert raw.cursor_instance.calls == [
        ("SELECT value FROM sample WHERE id = %s", (7,)),
        ("INSERT INTO sample(value) VALUES (%s)", [("one",), ("two",)]),
    ]


def test_postgres_adapter_closes_after_exception():
    raw = FakePostgresConnection()
    connection = Connection(raw, postgres=True)

    try:
        with connection:
            raise RuntimeError("test failure")
    except RuntimeError:
        pass

    assert raw.closed is True
    assert raw.exited[0] is RuntimeError
