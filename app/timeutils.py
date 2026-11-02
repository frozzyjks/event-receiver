from __future__ import annotations

from datetime import UTC, datetime
from typing import Final

TIMESTAMP_FORMAT: Final[str] = "%Y-%m-%dT%H:%M:%S.%fZ"


def utc_now() -> datetime:
    return datetime.now(UTC)


def format_timestamp(moment: datetime) -> str:
    if moment.tzinfo is None:
        raise ValueError("Naive datetimes are not supported; use timezone-aware UTC")
    return moment.astimezone(UTC).strftime(TIMESTAMP_FORMAT)