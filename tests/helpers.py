from __future__ import annotations

import sqlite3
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from app.task_queue import EnqueueResult, enqueue_event

START_TIME = datetime(2025, 1, 1, 12, 0, tzinfo=UTC)
WAIT_TIMEOUT_SECONDS = 5.0
WAIT_STEP_SECONDS = 0.01


class FakeClock:
    def __init__(self, start: datetime = START_TIME) -> None:
        self.now = start

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)


def add_event(
    connection: sqlite3.Connection,
    event_id: str = "evt-1",
    event_type: str = "payment_succeeded",
    payload: dict[str, Any] | None = None,
    now: datetime = START_TIME,
) -> EnqueueResult:
    body = payload if payload is not None else {"amount": 1000, "currency": "USD"}
    return enqueue_event(connection, event_id, event_type, body, now)


def get_task(connection: sqlite3.Connection, event_id: str) -> sqlite3.Row:
    row = connection.execute(
        "SELECT t.* FROM tasks t JOIN events e ON e.id = t.event_id "
        "WHERE e.event_id = ?",
        (event_id,),
    ).fetchone()
    assert row is not None, f"task for {event_id} not found"
    return row


def count_rows(connection: sqlite3.Connection, table: str) -> int:
    return connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def wait_until(condition: Callable[[], bool], timeout: float = WAIT_TIMEOUT_SECONDS) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(WAIT_STEP_SECONDS)
    return condition()