from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Final

from app.config import Settings

MS_IN_SECOND: Final[int] = 1000

SCHEMA: Final[str] = """
CREATE TABLE IF NOT EXISTS events (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id   TEXT NOT NULL UNIQUE CHECK (length(event_id) > 0),
    type       TEXT NOT NULL CHECK (length(type) > 0),
    payload    TEXT NOT NULL
               CHECK (json_valid(payload) AND json_type(payload) = 'object'),
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_events_type ON events (type);

CREATE TABLE IF NOT EXISTS tasks (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id      INTEGER NOT NULL UNIQUE
                  REFERENCES events (id) ON DELETE CASCADE,
    status        TEXT NOT NULL DEFAULT 'pending'
                  CHECK (status IN ('pending', 'success', 'failed', 'skipped')),
    attempts      INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_retry_at TEXT NOT NULL,
    last_error    TEXT,
    locked_at     TEXT,
    locked_by     TEXT,
    created_at    TEXT NOT NULL,
    updated_at    TEXT NOT NULL,
    CHECK ((locked_at IS NULL) = (locked_by IS NULL))
);

CREATE INDEX IF NOT EXISTS idx_tasks_pending
    ON tasks (next_retry_at, id) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks (status);
"""


@contextmanager
def connect(settings: Settings) -> Iterator[sqlite3.Connection]:
    connection = sqlite3.connect(
        settings.database_path,
        timeout=settings.busy_timeout_ms / MS_IN_SECOND,
        isolation_level=None,
    )
    try:
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        yield connection
    finally:
        connection.close()


@contextmanager
def transaction(connection: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    connection.execute("BEGIN IMMEDIATE")
    try:
        yield connection
    except BaseException:
        if connection.in_transaction:
            connection.execute("ROLLBACK")
        raise
    else:
        connection.execute("COMMIT")


def init_db(settings: Settings) -> None:
    with connect(settings) as connection:
        connection.execute("PRAGMA journal_mode = WAL")
        connection.executescript(SCHEMA)