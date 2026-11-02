from __future__ import annotations

import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

from app.config import RetryPolicy, Settings
from app.database import connect
from app.task_queue import TaskStatus, claim_next_task, complete_task
from app.timeutils import format_timestamp, utc_now
from app.worker import Worker, process_next_task
from tests.helpers import FakeClock, add_event, get_task, wait_until

WORKER_ID = "worker-1"
CONCURRENT_TASKS = 30


def run_once(
    db: sqlite3.Connection, policy: RetryPolicy, clock: FakeClock, worker_id: str = WORKER_ID
) -> bool:
    return process_next_task(db, worker_id, policy, clock)


def test_successful_task_gets_success_status(
    db: sqlite3.Connection, policy: RetryPolicy, clock: FakeClock
) -> None:
    add_event(db)

    assert run_once(db, policy, clock) is True

    task = get_task(db, "evt-1")
    assert task["status"] == "success"
    assert task["attempts"] == 1
    assert task["locked_by"] is None and task["locked_at"] is None
    assert task["last_error"] is None
    assert run_once(db, policy, clock) is False


def test_empty_queue_returns_false(
    db: sqlite3.Connection, policy: RetryPolicy, clock: FakeClock
) -> None:
    assert run_once(db, policy, clock) is False


def test_unknown_event_type_is_skipped(
    db: sqlite3.Connection, policy: RetryPolicy, clock: FakeClock
) -> None:
    add_event(db, event_type="something_else")

    run_once(db, policy, clock)

    task = get_task(db, "evt-1")
    assert task["status"] == "skipped"
    assert task["attempts"] == 1


def test_failed_attempt_is_retried_with_exponential_backoff(
    db: sqlite3.Connection, policy: RetryPolicy, clock: FakeClock
) -> None:
    add_event(db, payload={"simulate_failures": 2})

    run_once(db, policy, clock)
    task = get_task(db, "evt-1")
    assert task["status"] == "pending"
    assert task["attempts"] == 1
    assert task["next_retry_at"] == format_timestamp(clock.now + timedelta(seconds=1))
    assert "Simulated failure" in task["last_error"]
    assert task["locked_by"] is None

    assert run_once(db, policy, clock) is False
    clock.advance(1)

    run_once(db, policy, clock)
    task = get_task(db, "evt-1")
    assert task["status"] == "pending"
    assert task["attempts"] == 2
    assert task["next_retry_at"] == format_timestamp(clock.now + timedelta(seconds=2))

    clock.advance(1)
    assert run_once(db, policy, clock) is False
    clock.advance(1)

    run_once(db, policy, clock)
    task = get_task(db, "evt-1")
    assert task["status"] == "success"
    assert task["attempts"] == 3
    assert task["last_error"] is not None


def test_task_becomes_failed_after_three_attempts_and_is_never_picked_again(
    db: sqlite3.Connection, policy: RetryPolicy, clock: FakeClock
) -> None:
    add_event(db, payload={"simulate_failures": 3})

    run_once(db, policy, clock)
    clock.advance(1)
    run_once(db, policy, clock)
    clock.advance(2)
    run_once(db, policy, clock)

    task = get_task(db, "evt-1")
    assert task["status"] == "failed"
    assert task["attempts"] == 3
    assert "EventProcessingError" in task["last_error"]
    assert task["locked_by"] is None
    clock.advance(3600)
    assert run_once(db, policy, clock) is False
    assert get_task(db, "evt-1")["attempts"] == 3


def test_tasks_are_processed_in_creation_order(
    db: sqlite3.Connection, policy: RetryPolicy, clock: FakeClock
) -> None:
    for event_id in ("first", "second", "third"):
        add_event(db, event_id=event_id)

    claimed = []
    for _ in range(3):
        task = claim_next_task(db, WORKER_ID, clock.now, policy)
        assert task is not None
        claimed.append(task.event_id)
        complete_task(db, task, TaskStatus.SUCCESS, clock.now)

    assert claimed == ["first", "second", "third"]


def test_locked_task_is_not_given_to_another_worker(
    db: sqlite3.Connection, policy: RetryPolicy, clock: FakeClock
) -> None:
    add_event(db)
    assert claim_next_task(db, "worker-a", clock.now, policy) is not None

    assert claim_next_task(db, "worker-b", clock.now, policy) is None


def test_task_with_expired_lock_is_recovered_by_another_worker(
    db: sqlite3.Connection, policy: RetryPolicy, clock: FakeClock
) -> None:
    add_event(db)
    claim_next_task(db, "crashed-worker", clock.now, policy)

    clock.advance(policy.lock_timeout_seconds - 1)
    assert claim_next_task(db, "worker-b", clock.now, policy) is None

    clock.advance(2)
    recovered = claim_next_task(db, "worker-b", clock.now, policy)

    assert recovered is not None
    assert recovered.attempt == 2
    assert get_task(db, "evt-1")["locked_by"] == "worker-b"


def test_crashing_worker_cannot_keep_task_pending_forever(
    db: sqlite3.Connection, policy: RetryPolicy, clock: FakeClock
) -> None:
    add_event(db)

    for _ in range(policy.max_attempts):
        assert claim_next_task(db, "crashed-worker", clock.now, policy) is not None
        clock.advance(policy.lock_timeout_seconds + 1)

    assert claim_next_task(db, "worker-b", clock.now, policy) is None
    task = get_task(db, "evt-1")
    assert task["status"] == "failed"
    assert task["attempts"] == policy.max_attempts
    assert "Lock expired" in task["last_error"]
    assert task["locked_by"] is None


def test_result_of_worker_that_lost_its_lock_is_discarded(
    db: sqlite3.Connection, policy: RetryPolicy, clock: FakeClock
) -> None:
    add_event(db)
    slow_task = claim_next_task(db, "slow-worker", clock.now, policy)
    clock.advance(policy.lock_timeout_seconds + 1)
    new_task = claim_next_task(db, "fast-worker", clock.now, policy)
    assert slow_task is not None and new_task is not None

    assert complete_task(db, slow_task, TaskStatus.SUCCESS, clock.now) is False
    assert get_task(db, "evt-1")["status"] == "pending"
    assert complete_task(db, new_task, TaskStatus.SUCCESS, clock.now) is True
    assert get_task(db, "evt-1")["status"] == "success"


def test_two_workers_never_process_the_same_task(
    settings: Settings, db: sqlite3.Connection
) -> None:
    now = utc_now()
    for index in range(CONCURRENT_TASKS):
        add_event(db, event_id=f"evt-{index}", now=now)
    barrier = threading.Barrier(2)

    def drain(worker_id: str) -> int:
        processed = 0
        with connect(settings) as connection:
            barrier.wait()
            while process_next_task(connection, worker_id, settings.retry_policy):
                processed += 1
        return processed

    with ThreadPoolExecutor(max_workers=2) as pool:
        counts = [f.result() for f in [pool.submit(drain, "w-a"), pool.submit(drain, "w-b")]]

    assert sum(counts) == CONCURRENT_TASKS
    rows = db.execute("SELECT status, attempts FROM tasks").fetchall()
    assert all(row["status"] == "success" and row["attempts"] == 1 for row in rows)


def test_worker_processes_queue_and_stops_gracefully(
    settings: Settings, db: sqlite3.Connection
) -> None:
    add_event(db, now=utc_now())
    worker = Worker(settings, worker_id="threaded-worker")
    thread = threading.Thread(target=worker.run_forever)

    thread.start()
    try:
        processed = wait_until(lambda: get_task(db, "evt-1")["status"] == "success")
    finally:
        worker.stop()
        thread.join(timeout=5)

    assert processed
    assert not thread.is_alive()