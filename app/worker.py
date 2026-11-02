from __future__ import annotations

import logging
import os
import signal
import sqlite3
import sys
import threading
import uuid
from collections.abc import Callable
from datetime import datetime
from types import FrameType

from app.config import RetryPolicy, Settings, configure_logging
from app.database import connect, init_db
from app.processor import UnknownEventTypeError, process_event
from app.task_queue import (
    ClaimedTask,
    TaskStatus,
    claim_next_task,
    complete_task,
    fail_task,
)
from app.timeutils import utc_now

logger = logging.getLogger(__name__)

Clock = Callable[[], datetime]


def process_next_task(
    connection: sqlite3.Connection,
    worker_id: str,
    policy: RetryPolicy,
    now_fn: Clock = utc_now,
) -> bool:
    task = claim_next_task(connection, worker_id, now_fn(), policy)
    if task is None:
        return False
    _run_task(connection, task, policy, now_fn)
    return True


def _run_task(
    connection: sqlite3.Connection,
    task: ClaimedTask,
    policy: RetryPolicy,
    now_fn: Clock,
) -> None:
    try:
        process_event(task.event_type, task.payload, task.attempt)
    except UnknownEventTypeError as exc:
        logger.info("Task %d skipped: %s", task.task_id, exc)
        complete_task(connection, task, TaskStatus.SKIPPED, now_fn())
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        logger.warning(
            "Task %d failed on attempt %d/%d: %s",
            task.task_id,
            task.attempt,
            policy.max_attempts,
            error,
        )
        fail_task(connection, task, error, now_fn(), policy)
    else:
        logger.info("Task %d succeeded on attempt %d", task.task_id, task.attempt)
        complete_task(connection, task, TaskStatus.SUCCESS, now_fn())


class Worker:

    def __init__(self, settings: Settings, worker_id: str | None = None) -> None:
        self._settings = settings
        self._worker_id = worker_id or f"{os.getpid()}-{uuid.uuid4().hex[:8]}"
        self._stop_event = threading.Event()

    @property
    def worker_id(self) -> str:
        return self._worker_id

    def stop(self) -> None:
        self._stop_event.set()

    def run_forever(self) -> None:
        logger.info("Worker %s started", self._worker_id)
        try:
            while not self._stop_event.is_set():
                self._poll_once()
                self._stop_event.wait(self._settings.poll_interval_seconds)
        finally:
            logger.info("Worker %s stopped", self._worker_id)

    def _poll_once(self) -> None:
        try:
            with connect(self._settings) as connection:
                while not self._stop_event.is_set() and process_next_task(
                    connection, self._worker_id, self._settings.retry_policy
                ):
                    pass
        except sqlite3.Error:
            logger.exception("Database error in worker; will retry on next poll")
        except Exception:
            logger.exception("Unexpected worker error; will retry on next poll")


def main() -> int:
    settings = Settings.from_env()
    configure_logging(settings.log_level)
    init_db(settings)

    worker = Worker(settings)

    def request_stop(signum: int, frame: FrameType | None) -> None:
        worker.stop()

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    worker.run_forever()
    return 0


if __name__ == "__main__":
    sys.exit(main())