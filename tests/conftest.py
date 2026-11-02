from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from pathlib import Path

import pytest
from flask import Flask
from flask.testing import FlaskClient

from app import create_app
from app.config import RetryPolicy, Settings
from app.database import connect, init_db
from tests.helpers import FakeClock


@pytest.fixture
def policy() -> RetryPolicy:
    return RetryPolicy(max_attempts=3, base_delay_seconds=1.0, lock_timeout_seconds=30.0)


@pytest.fixture
def settings(tmp_path: Path, policy: RetryPolicy) -> Settings:
    return Settings(
        database_path=str(tmp_path / "test.db"),
        retry_policy=policy,
        poll_interval_seconds=0.01,
    )


@pytest.fixture
def db(settings: Settings) -> Iterator[sqlite3.Connection]:
    init_db(settings)
    with connect(settings) as connection:
        yield connection


@pytest.fixture
def app(settings: Settings, db: sqlite3.Connection) -> Flask:
    return create_app(settings)


@pytest.fixture
def client(app: Flask) -> FlaskClient:
    return app.test_client()


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()