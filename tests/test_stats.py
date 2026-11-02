from __future__ import annotations

import sqlite3

from flask.testing import FlaskClient

from app.config import RetryPolicy
from app.worker import process_next_task

ZERO_TASKS = {"pending": 0, "success": 0, "failed": 0, "skipped": 0}


def post_event(client: FlaskClient, event_id: str, event_type: str) -> None:
    response = client.post(
        "/webhook", json={"event_id": event_id, "type": event_type, "payload": {}}
    )
    assert response.status_code == 200


def test_stats_on_empty_database_has_all_task_statuses_with_zero(client: FlaskClient) -> None:
    response = client.get("/stats")

    assert response.status_code == 200
    assert response.get_json() == {"events": {}, "tasks": ZERO_TASKS}


def test_stats_reflect_events_and_task_statuses(
    client: FlaskClient, db: sqlite3.Connection, policy: RetryPolicy
) -> None:
    post_event(client, "e1", "payment_succeeded")
    post_event(client, "e2", "payment_failed")
    post_event(client, "e3", "mystery")
    post_event(client, "e4", "payment_succeeded")
    post_event(client, "e4", "payment_succeeded")
    for _ in range(3):
        assert process_next_task(db, "w", policy) is True

    body = client.get("/stats").get_json()

    assert body["events"] == {"mystery": 1, "payment_failed": 1, "payment_succeeded": 2}
    assert body["tasks"] == {**ZERO_TASKS, "pending": 1, "success": 2, "skipped": 1}