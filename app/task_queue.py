from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from typing import Any, Final

from app.config import RetryPolicy
from app.database import transaction
from app.timeutils import format_timestamp

logger = logging.getLogger(__name__)

LOCK_EXPIRED_ERROR: Final[str] = (
    "Lock expired"
)


class TaskStatus(StrEnum):

    PENDING = "pending"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


FINAL_STATUSES: Final[frozenset[TaskStatus]] = frozenset(
    {TaskStatus.SUCCESS, TaskStatus.SKIPPED}
)


@dataclass(frozen=True)
class EnqueueResult:

    task_id: int
    created: bool


@dataclass(frozen=True)
class ClaimedTask:

    task_id: int
    event_id: str
    event_type: str
    payload: dict[str, Any]
    attempt: int
    worker_id: str


_INSERT_EVENT_SQL: Final[str] = """
INSERT INTO events (event_id, type, payload, created_at)
VALUES (:event_id, :type, :payload, :now)
ON CONFLICT (event_id) DO NOTHING
"""

_INSERT_TASK_SQL: Final[str] = """
INSERT INTO tasks (event_id, status, attempts, next_retry_at, created_at, updated_at)
VALUES (:event_pk, 'pending', 0, :now, :now, :now)
"""

_SELECT_TASK_BY_EVENT_SQL: Final[str] = """
SELECT t.id FROM tasks t JOIN events e ON e.id = t.event_id
WHERE e.event_id = :event_id
"""

_FAIL_EXHAUSTED_SQL: Final[str] = """
UPDATE tasks
SET status = 'failed', locked_at = NULL, locked_by = NULL,
    last_error = :error, updated_at = :now
WHERE status = 'pending'
  AND attempts >= :max_attempts
  AND (locked_at IS NULL OR locked_at <= :stale_before)
"""

_SELECT_CLAIMABLE_SQL: Final[str] = """
SELECT t.id AS task_id, t.attempts AS attempts,
       e.event_id AS external_event_id, e.type AS event_type, e.payload AS payload
FROM tasks t JOIN events e ON e.id = t.event_id
WHERE t.status = 'pending'
  AND t.next_retry_at <= :now
  AND (t.locked_at IS NULL OR t.locked_at <= :stale_before)
ORDER BY t.id
LIMIT 1
"""

_LOCK_TASK_SQL: Final[str] = """
UPDATE tasks
SET attempts = attempts + 1, locked_at = :now, locked_by = :worker_id,
    updated_at = :now
WHERE id = :task_id
"""

_RELEASE_TASK_SQL: Final[str] = """
UPDATE tasks
SET status = :status,
    next_retry_at = COALESCE(:next_retry_at, next_retry_at),
    last_error = COALESCE(:error, last_error),
    locked_at = NULL, locked_by = NULL, updated_at = :now
WHERE id = :task_id AND locked_by = :worker_id AND status = 'pending'
"""

_STATS_SQL: Final[str] = """
SELECT 'event' AS kind, type AS name, COUNT(*) AS total FROM events GROUP BY type
UNION ALL
SELECT 'task' AS kind, status AS name, COUNT(*) AS total FROM tasks GROUP BY status
"""


def enqueue_event(
    connection: sqlite3.Connection,
    event_id: str,
    event_type: str,
    payload: dict[str, Any],
    now: datetime,
) -> EnqueueResult:
    params = {
        "event_id": event_id,
        "type": event_type,
        "payload": json.dumps(payload, ensure_ascii=False, allow_nan=False),
        "now": format_timestamp(now),
    }
    with transaction(connection):
        cursor = connection.execute(_INSERT_EVENT_SQL, params)
        if cursor.rowcount == 0:
            existing = connection.execute(
                _SELECT_TASK_BY_EVENT_SQL, {"event_id": event_id}
            ).fetchone()
            return EnqueueResult(task_id=existing["id"], created=False)
        task_cursor = connection.execute(
            _INSERT_TASK_SQL, {"event_pk": _inserted_id(cursor), "now": params["now"]}
        )
        return EnqueueResult(task_id=_inserted_id(task_cursor), created=True)


def claim_next_task(
    connection: sqlite3.Connection,
    worker_id: str,
    now: datetime,
    policy: RetryPolicy,
) -> ClaimedTask | None:
    now_text = format_timestamp(now)
    stale_before = format_timestamp(now - timedelta(seconds=policy.lock_timeout_seconds))
    with transaction(connection):
        _fail_exhausted_tasks(connection, now_text, stale_before, policy.max_attempts)
        row = connection.execute(
            _SELECT_CLAIMABLE_SQL, {"now": now_text, "stale_before": stale_before}
        ).fetchone()
        if row is None:
            return None
        connection.execute(
            _LOCK_TASK_SQL,
            {"now": now_text, "worker_id": worker_id, "task_id": row["task_id"]},
        )
        return ClaimedTask(
            task_id=row["task_id"],
            event_id=row["external_event_id"],
            event_type=row["event_type"],
            payload=json.loads(row["payload"]),
            attempt=row["attempts"] + 1,
            worker_id=worker_id,
        )


def complete_task(
    connection: sqlite3.Connection,
    task: ClaimedTask,
    status: TaskStatus,
    now: datetime,
) -> bool:
    if status not in FINAL_STATUSES:
        raise ValueError(f"complete_task accepts only {sorted(FINAL_STATUSES)}")
    return _release_task(connection, task, status, now)


def fail_task(
    connection: sqlite3.Connection,
    task: ClaimedTask,
    error: str,
    now: datetime,
    policy: RetryPolicy,
) -> bool:
    if task.attempt >= policy.max_attempts:
        return _release_task(connection, task, TaskStatus.FAILED, now, error=error)
    next_retry_at = now + policy.delay_for_attempt(task.attempt)
    return _release_task(
        connection, task, TaskStatus.PENDING, now, error=error, next_retry_at=next_retry_at
    )


def get_stats(connection: sqlite3.Connection) -> dict[str, dict[str, int]]:
    events: dict[str, int] = {}
    tasks: dict[str, int] = {status.value: 0 for status in TaskStatus}
    for row in connection.execute(_STATS_SQL):
        target = events if row["kind"] == "event" else tasks
        target[row["name"]] = row["total"]
    return {"events": dict(sorted(events.items())), "tasks": tasks}


def _inserted_id(cursor: sqlite3.Cursor) -> int:
    if cursor.lastrowid is None:
        raise RuntimeError("INSERT did not produce a row id")
    return cursor.lastrowid


def _fail_exhausted_tasks(
    connection: sqlite3.Connection, now_text: str, stale_before: str, max_attempts: int
) -> None:
    cursor = connection.execute(
        _FAIL_EXHAUSTED_SQL,
        {
            "error": LOCK_EXPIRED_ERROR,
            "now": now_text,
            "stale_before": stale_before,
            "max_attempts": max_attempts,
        },
    )
    if cursor.rowcount:
        logger.warning("Marked %d abandoned task(s) as failed", cursor.rowcount)


def _release_task(
    connection: sqlite3.Connection,
    task: ClaimedTask,
    status: TaskStatus,
    now: datetime,
    error: str | None = None,
    next_retry_at: datetime | None = None,
) -> bool:
    cursor = connection.execute(
        _RELEASE_TASK_SQL,
        {
            "status": status.value,
            "next_retry_at": format_timestamp(next_retry_at) if next_retry_at else None,
            "error": error,
            "now": format_timestamp(now),
            "task_id": task.task_id,
            "worker_id": task.worker_id,
        },
    )
    released = cursor.rowcount == 1
    if not released:
        logger.warning(
            "Task %d was not released by %s: lock was lost", task.task_id, task.worker_id
        )
    return released