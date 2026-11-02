from __future__ import annotations

import pytest

from app.processor import (
    EventProcessingError,
    UnknownEventTypeError,
    process_event,
)


@pytest.mark.parametrize("event_type", ["payment_succeeded", "payment_failed"])
def test_known_event_types_are_processed(event_type: str) -> None:
    process_event(event_type, {"amount": 5, "currency": "EUR"}, attempt=1)


def test_unknown_event_type_raises_dedicated_error() -> None:
    with pytest.raises(UnknownEventTypeError):
        process_event("mystery", {}, attempt=1)


def test_simulated_failures_apply_only_to_first_n_attempts() -> None:
    payload = {"simulate_failures": 2}

    for attempt in (1, 2):
        with pytest.raises(EventProcessingError):
            process_event("payment_succeeded", payload, attempt)
    process_event("payment_succeeded", payload, attempt=3)


@pytest.mark.parametrize("value", [True, "2", 1.5, None, 0, -1])
def test_non_positive_or_non_integer_simulation_values_are_ignored(value: object) -> None:
    process_event("payment_succeeded", {"simulate_failures": value}, attempt=1)