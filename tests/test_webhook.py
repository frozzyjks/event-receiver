from __future__ import annotations

import json
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pytest
from flask.testing import FlaskClient

from app.config import Settings
from app.database import connect
from app.task_queue import enqueue_event
from app.timeutils import utc_now
from tests.helpers import count_rows, get_task

VALID_BODY: dict[str, Any] = {
    "event_id": "abc-123",
    "type": "payment_succeeded",
    "payload": {"amount": 1000, "currency": "USD"},
}
CONCURRENT_REQUESTS = 8


def post_json(client: FlaskClient, body: Any):
    return client.post("/webhook", data=json.dumps(body), content_type="application/json")


def test_valid_webhook_creates_event_and_pending_task(
    client: FlaskClient, db: sqlite3.Connection
) -> None:
    response = post_json(client, VALID_BODY)

    assert response.status_code == 200
    assert response.get_json()["status"] == "accepted"
    event = db.execute("SELECT * FROM events WHERE event_id = 'abc-123'").fetchone()
    assert event["type"] == "payment_succeeded"
    assert json.loads(event["payload"]) == VALID_BODY["payload"]
    task = get_task(db, "abc-123")
    assert task["status"] == "pending"
    assert task["attempts"] == 0
    assert task["locked_by"] is None


def test_duplicate_event_id_does_not_create_new_rows(
    client: FlaskClient, db: sqlite3.Connection
) -> None:
    first = post_json(client, VALID_BODY)
    second = post_json(client, {**VALID_BODY, "payload": {"amount": 1}})

    assert second.status_code == 200
    body = second.get_json()
    assert body["status"] == "duplicate"
    assert "already exists" in body["message"]
    assert body["task_id"] == first.get_json()["task_id"]
    assert count_rows(db, "events") == 1
    assert count_rows(db, "tasks") == 1
    stored = db.execute("SELECT payload FROM events").fetchone()["payload"]
    assert json.loads(stored) == VALID_BODY["payload"]


def test_malformed_json_returns_400_and_writes_nothing(
    client: FlaskClient, db: sqlite3.Connection
) -> None:
    response = client.post("/webhook", data="{not json", content_type="application/json")

    assert response.status_code == 400
    assert "error" in response.get_json()
    assert count_rows(db, "events") == 0
    assert count_rows(db, "tasks") == 0


@pytest.mark.parametrize(
    "body",
    [
        None,
        [],
        "text",
        {},
        {**VALID_BODY, "event_id": None},
        {**VALID_BODY, "event_id": ""},
        {**VALID_BODY, "event_id": "   "},
        {**VALID_BODY, "event_id": 123},
        {**VALID_BODY, "event_id": "x" * 256},
        {**VALID_BODY, "type": ""},
        {**VALID_BODY, "type": ["payment_succeeded"]},
        {"event_id": "abc-123", "payload": {}},
        {"event_id": "abc-123", "type": "t"},
        {**VALID_BODY, "payload": []},
        {**VALID_BODY, "payload": "string"},
        {**VALID_BODY, "payload": None},
    ],
    ids=lambda body: json.dumps(body)[:40],
)
def test_invalid_structure_returns_400_and_writes_nothing(
    client: FlaskClient, db: sqlite3.Connection, body: Any
) -> None:
    response = post_json(client, body)

    assert response.status_code == 400
    assert response.get_json()["error"]
    assert count_rows(db, "events") == 0
    assert count_rows(db, "tasks") == 0


def test_nan_in_payload_is_rejected(client: FlaskClient, db: sqlite3.Connection) -> None:
    raw = '{"event_id": "e", "type": "t", "payload": {"value": NaN}}'

    response = client.post("/webhook", data=raw, content_type="application/json")

    assert response.status_code == 400
    assert count_rows(db, "events") == 0


def test_empty_payload_object_is_accepted(client: FlaskClient) -> None:
    response = post_json(client, {**VALID_BODY, "payload": {}})

    assert response.status_code == 200


def test_unknown_route_returns_json_404(client: FlaskClient) -> None:
    response = client.get("/nope")

    assert response.status_code == 404
    assert "error" in response.get_json()


def test_concurrent_identical_events_create_single_event_and_task(
    settings: Settings, db: sqlite3.Connection
) -> None:
    barrier = threading.Barrier(CONCURRENT_REQUESTS)

    def enqueue_from_own_connection() -> bool:
        with connect(settings) as connection:
            barrier.wait()
            return enqueue_event(
                connection, "same-id", "payment_succeeded", {"amount": 1}, utc_now()
            ).created

    with ThreadPoolExecutor(max_workers=CONCURRENT_REQUESTS) as pool:
        futures = [pool.submit(enqueue_from_own_connection) for _ in range(CONCURRENT_REQUESTS)]
        created_flags = [future.result() for future in futures]

    assert created_flags.count(True) == 1
    assert count_rows(db, "events") == 1
    assert count_rows(db, "tasks") == 1