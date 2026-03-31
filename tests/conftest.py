# tests/conftest.py
"""Shared fixtures for pitwall tests."""

import asyncio
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import pytest

from pitwall.api_handler.client import AsyncDirectClient, DirectClient
from pitwall.api_handler.settings import ClientSettings

DATA_DIR = Path(__file__).parent / "data"


# ── Retry backoff elimination ─────────────────────────────────────────
# Without this, tenacity waits real seconds between retries in tests.


@pytest.fixture(autouse=True)
def no_retry_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    """Kill tenacity/asyncio backoff so retries are instant."""
    original_sleep = asyncio.sleep

    async def instant_sleep(delay: float, *args: object, **kwargs: object) -> None:  # pyright: ignore[reportUnusedParameter]
        await original_sleep(0)

    monkeypatch.setattr(asyncio, "sleep", instant_sleep)

    import time

    monkeypatch.setattr(time, "sleep", lambda _: None)  # pyright: ignore[reportUnknownLambdaType, reportUnknownArgumentType]


# ── Canned response data ─────────────────────────────────────────────
# Load once per test run. These should be real (or trimmed) API responses
# captured via curl and saved in tests/data/.


@pytest.fixture(scope="session")
def year_index() -> str:
    """Root Index.json — lists available years."""
    return (DATA_DIR / "base_index.json").read_text()


@pytest.fixture(scope="session")
def season_index() -> str:
    """Year-level Index.json — lists meetings for a season.
    curl 'https://livetiming.formula1.com/static/2023/Index.json' > tests/data/season_index.json
    """
    return (DATA_DIR / "season_index.json").read_text()


@pytest.fixture(scope="session")
def timing_data_keyframe() -> str:
    """TimingData keyframe JSON.
    curl 'https://livetiming.formula1.com/static/2023/{meeting}/{session}/TimingData.jsonStream' > tests/data/timing_data_keyframe.json
    """
    return (DATA_DIR / "timing_data.json").read_text()


@pytest.fixture(scope="session")
def timing_data_stream() -> str:
    """TimingData stream (jsonStream format with timestamp prefixes).
    curl 'https://livetiming.formula1.com/static/2023/{meeting}/{session}/TimingData.jsonStream' > tests/data/timing_data_stream.jsonl
    """
    return (DATA_DIR / "timing_data.jsonStream").read_text()


# ── Client instances ──────────────────────────────────────────────────


@pytest.fixture(scope="module")
def settings() -> ClientSettings:
    """Test settings with short timeouts."""
    return ClientSettings(
        connect=1.0,
        read=2.0,
    )


@pytest.fixture
def sync_client(settings: ClientSettings) -> Iterator[DirectClient]:
    with DirectClient(settings=settings) as client:
        yield client


@pytest.fixture
async def async_client(settings: ClientSettings) -> AsyncIterator[AsyncDirectClient]:
    async with AsyncDirectClient(settings=settings) as client:
        yield client
