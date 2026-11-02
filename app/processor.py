from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from typing import Any, Final

logger = logging.getLogger(__name__)

SIMULATED_FAILURES_KEY: Final[str] = "simulate_failures"

EventHandler = Callable[[Mapping[str, Any]], None]


class EventProcessingError(Exception):
    """Raised when an event cannot be processed; the task will be retried"""


class UnknownEventTypeError(Exception):
    """Raised for event types without a handler; the task will be skipped"""


def process_event(event_type: str, payload: Mapping[str, Any], attempt: int) -> None:
    handler = _HANDLERS.get(event_type)
    if handler is None:
        raise UnknownEventTypeError(f"No handler for event type {event_type!r}")
    _raise_if_failure_simulated(payload, attempt)
    handler(payload)


def _raise_if_failure_simulated(payload: Mapping[str, Any], attempt: int) -> None:
    failures = payload.get(SIMULATED_FAILURES_KEY)
    if isinstance(failures, bool) or not isinstance(failures, int):
        return
    if attempt <= failures:
        raise EventProcessingError(
            f"Simulated failure (attempt {attempt}, {SIMULATED_FAILURES_KEY}={failures})"
        )


def _handle_payment_succeeded(payload: Mapping[str, Any]) -> None:
    logger.info(
        "Payment succeeded: amount=%s currency=%s",
        payload.get("amount"),
        payload.get("currency"),
    )


def _handle_payment_failed(payload: Mapping[str, Any]) -> None:
    logger.info(
        "Payment failed: amount=%s currency=%s reason=%s",
        payload.get("amount"),
        payload.get("currency"),
        payload.get("reason"),
    )


_HANDLERS: Final[dict[str, EventHandler]] = {
    "payment_succeeded": _handle_payment_succeeded,
    "payment_failed": _handle_payment_failed,
}