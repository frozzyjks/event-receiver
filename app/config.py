from __future__ import annotations

import logging
import os
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Final, TypeVar

SETTINGS_CONFIG_KEY: Final[str] = "EVENTS_SETTINGS"

DEFAULT_DATABASE_PATH: Final[str] = "events.db"
DEFAULT_MAX_ATTEMPTS: Final[int] = 3
DEFAULT_RETRY_BASE_DELAY_SECONDS: Final[float] = 1.0
DEFAULT_LOCK_TIMEOUT_SECONDS: Final[float] = 30.0
DEFAULT_POLL_INTERVAL_SECONDS: Final[float] = 1.0
DEFAULT_BUSY_TIMEOUT_MS: Final[int] = 5_000
DEFAULT_MAX_CONTENT_LENGTH: Final[int] = 1024 * 1024
DEFAULT_LOG_LEVEL: Final[str] = "INFO"
BACKOFF_FACTOR: Final[int] = 2
LOG_FORMAT: Final[str] = "%(asctime)s %(levelname)s %(name)s: %(message)s"

_T = TypeVar("_T")


@dataclass(frozen=True)
class RetryPolicy:

    max_attempts: int = DEFAULT_MAX_ATTEMPTS
    base_delay_seconds: float = DEFAULT_RETRY_BASE_DELAY_SECONDS
    lock_timeout_seconds: float = DEFAULT_LOCK_TIMEOUT_SECONDS

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if self.base_delay_seconds <= 0:
            raise ValueError("base_delay_seconds must be > 0")
        if self.lock_timeout_seconds <= 0:
            raise ValueError("lock_timeout_seconds must be > 0")

    def delay_for_attempt(self, attempt: int) -> timedelta:
        if attempt < 1:
            raise ValueError("attempt must be >= 1")
        return timedelta(
            seconds=self.base_delay_seconds * BACKOFF_FACTOR ** (attempt - 1)
        )


@dataclass(frozen=True)
class Settings:

    database_path: str = DEFAULT_DATABASE_PATH
    retry_policy: RetryPolicy = field(default_factory=RetryPolicy)
    poll_interval_seconds: float = DEFAULT_POLL_INTERVAL_SECONDS
    busy_timeout_ms: int = DEFAULT_BUSY_TIMEOUT_MS
    max_content_length: int = DEFAULT_MAX_CONTENT_LENGTH
    log_level: str = DEFAULT_LOG_LEVEL

    def __post_init__(self) -> None:
        if not self.database_path:
            raise ValueError("database_path must not be empty")
        if self.poll_interval_seconds <= 0:
            raise ValueError("poll_interval_seconds must be > 0")
        if self.busy_timeout_ms < 0:
            raise ValueError("busy_timeout_ms must be >= 0")
        if self.max_content_length <= 0:
            raise ValueError("max_content_length must be > 0")

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            database_path=_read_env("EVENTS_DB_PATH", DEFAULT_DATABASE_PATH, str),
            retry_policy=RetryPolicy(
                max_attempts=_read_env("MAX_ATTEMPTS", DEFAULT_MAX_ATTEMPTS, int),
                base_delay_seconds=_read_env(
                    "RETRY_BASE_DELAY_SECONDS", DEFAULT_RETRY_BASE_DELAY_SECONDS, float
                ),
                lock_timeout_seconds=_read_env(
                    "LOCK_TIMEOUT_SECONDS", DEFAULT_LOCK_TIMEOUT_SECONDS, float
                ),
            ),
            poll_interval_seconds=_read_env(
                "WORKER_POLL_INTERVAL_SECONDS", DEFAULT_POLL_INTERVAL_SECONDS, float
            ),
            busy_timeout_ms=_read_env(
                "SQLITE_BUSY_TIMEOUT_MS", DEFAULT_BUSY_TIMEOUT_MS, int
            ),
            max_content_length=_read_env(
                "MAX_CONTENT_LENGTH", DEFAULT_MAX_CONTENT_LENGTH, int
            ),
            log_level=_read_env("LOG_LEVEL", DEFAULT_LOG_LEVEL, str),
        )


def _read_env(name: str, default: _T, cast: Callable[[str], _T]) -> _T:
    raw_value = os.environ.get(name)
    if raw_value is None or raw_value == "":
        return default
    try:
        return cast(raw_value)
    except ValueError as exc:
        raise ValueError(
            f"Environment variable {name} has invalid value {raw_value!r}"
        ) from exc


def configure_logging(level_name: str) -> None:
    logging.basicConfig(level=level_name.upper(), format=LOG_FORMAT)