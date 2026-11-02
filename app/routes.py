from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass
from http import HTTPStatus
from typing import Any, Final, cast

from flask import Blueprint, Flask, current_app, jsonify, request
from flask.typing import ResponseReturnValue
from werkzeug.exceptions import HTTPException

from app.config import SETTINGS_CONFIG_KEY, Settings
from app.database import connect
from app.task_queue import enqueue_event, get_stats
from app.timeutils import utc_now

logger = logging.getLogger(__name__)

MAX_EVENT_ID_LENGTH: Final[int] = 255
MAX_EVENT_TYPE_LENGTH: Final[int] = 100
RETRY_AFTER_SECONDS: Final[int] = 1

api = Blueprint("api", __name__)


class ValidationError(ValueError):
    """Raised when the webhook request body is malformed"""


@dataclass(frozen=True)
class WebhookEvent:

    event_id: str
    event_type: str
    payload: dict[str, Any]


def parse_webhook_event(body: object) -> WebhookEvent:
    if not isinstance(body, dict):
        raise ValidationError("Request body must be a valid JSON object")
    return WebhookEvent(
        event_id=_read_string_field(body, "event_id", MAX_EVENT_ID_LENGTH),
        event_type=_read_string_field(body, "type", MAX_EVENT_TYPE_LENGTH),
        payload=_read_payload(body),
    )


def _read_string_field(body: dict[str, Any], name: str, max_length: int) -> str:
    value = body.get(name)
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"Field '{name}' is required and must be a non-empty string")
    if len(value) > max_length:
        raise ValidationError(f"Field '{name}' must not exceed {max_length} characters")
    return value


def _read_payload(body: dict[str, Any]) -> dict[str, Any]:
    payload = body.get("payload")
    if not isinstance(payload, dict):
        raise ValidationError("Field 'payload' is required and must be a JSON object")
    try:
        json.dumps(payload, allow_nan=False)
    except ValueError as exc:
        raise ValidationError("Field 'payload' must not contain NaN or Infinity") from exc
    return payload


def _settings() -> Settings:
    return cast(Settings, current_app.config[SETTINGS_CONFIG_KEY])


@api.post("/webhook")
def receive_webhook() -> ResponseReturnValue:
    try:
        event = parse_webhook_event(request.get_json(force=True, silent=True))
    except ValidationError as exc:
        logger.info("Rejected webhook request: %s", exc)
        return jsonify(error=str(exc)), HTTPStatus.BAD_REQUEST

    with connect(_settings()) as connection:
        result = enqueue_event(
            connection, event.event_id, event.event_type, event.payload, utc_now()
        )

    if result.created:
        logger.info("Accepted event %s (task %d)", event.event_id, result.task_id)
        status, message = "accepted", "Event accepted and queued for processing"
    else:
        logger.info("Duplicate event %s ignored", event.event_id)
        status, message = "duplicate", "Event already exists"
    return (
        jsonify(
            status=status,
            message=message,
            event_id=event.event_id,
            task_id=result.task_id,
        ),
        HTTPStatus.OK,
    )


@api.get("/stats")
def stats() -> ResponseReturnValue:
    with connect(_settings()) as connection:
        return jsonify(get_stats(connection)), HTTPStatus.OK


def register_error_handlers(app: Flask) -> None:

    @app.errorhandler(HTTPException)
    def handle_http_error(error: HTTPException) -> ResponseReturnValue:
        return jsonify(error=error.description or error.name), error.code or 500

    @app.errorhandler(sqlite3.OperationalError)
    def handle_database_busy(error: sqlite3.OperationalError) -> ResponseReturnValue:
        logger.error("Database unavailable: %s", error)
        response = jsonify(error="Storage is temporarily unavailable, retry later")
        response.headers["Retry-After"] = str(RETRY_AFTER_SECONDS)
        return response, HTTPStatus.SERVICE_UNAVAILABLE