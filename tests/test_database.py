from __future__ import annotations

import sqlite3
from dataclasses import replace

import pytest

from app.config import Settings
from app.database import connect, transaction
from tests.helpers import add_event, count_rows


def test_foreign_keys_are_enabled_on_every_connection(db: sqlite3.Connection) -> None:
    assert db.execute("PRAGMA foreign_keys").fetchone()[0] == 1


def test_task_cannot_reference_missing_event(db: sqlite3.Connection) -> None:
    with pytest.raises(sqlite3.IntegrityError):
        db.execute(
            "INSERT INTO tasks (event_id, next_retry_at, created_at, updated_at) "
            "VALUES (999, 't', 't', 't')"
        )


def test_event_can_have_only_one_task(db: sqlite3.Connection) -> None:
    add_event(db)

    with pytest.raises(sqlite3.IntegrityError):
        db.execute(
            "INSERT INTO tasks (event_id, next_retry_at, created_at, updated_at) "
            "SELECT id, 't', 't', 't' FROM events"
        )


def test_invalid_task_status_is_rejected(db: sqlite3.Connection) -> None:
    add_event(db)

    with pytest.raises(sqlite3.IntegrityError):
        db.execute("UPDATE tasks SET status = 'bogus'")


def test_transaction_rolls_back_on_error(db: sqlite3.Connection) -> None:
    with pytest.raises(RuntimeError), transaction(db):
        db.execute(
            "INSERT INTO events (event_id, type, payload, created_at) "
            "VALUES ('e', 't', '{}', 'now')"
        )
        raise RuntimeError("boom")

    assert count_rows(db, "events") == 0
    assert not db.in_transaction


def test_transaction_takes_write_lock_immediately(
    settings: Settings, db: sqlite3.Connection
) -> None:
    no_wait_settings = replace(settings, busy_timeout_ms=0)

    with (
        transaction(db),
        connect(no_wait_settings) as other,
        pytest.raises(sqlite3.OperationalError, match="locked"),
    ):
        other.execute("BEGIN IMMEDIATE")