# tests/test_client_async.py
"""Tests for AsyncDirectClient — async HTTP client with TaskGroup."""

import httpx
import pytest
from pytest_httpx import HTTPXMock

from pitwall.api_handler.client import AsyncDirectClient
from pitwall.api_handler.settings import ClientSettings

BASE = "https://livetiming.formula1.com/static"


class TestFetch:
    """AsyncDirectClient.fetch() — single HTTP call behavior."""

    # ── Happy paths ───────────────────────────────────────────────

    async def test_no_args_fetches_root_index(
        self,
        async_client: AsyncDirectClient,
        httpx_mock: HTTPXMock,
        year_index: str,
    ) -> None:
        httpx_mock.add_response(text=year_index)
        result = await async_client.fetch()

        request = httpx_mock.get_request()
        assert request is not None
        assert str(request.url) == f"{BASE}/Index.json"
        assert isinstance(result, list)

    async def test_year_fetches_season_index(
        self,
        async_client: AsyncDirectClient,
        httpx_mock: HTTPXMock,
        season_index: str,
    ) -> None:
        httpx_mock.add_response(text=season_index)
        result = await async_client.fetch(year=2023)

        request = httpx_mock.get_request()
        assert request is not None
        assert str(request.url) == f"{BASE}/2023/Index.json"
        assert len(result) > 0

    async def test_meeting_triggers_resolution(
        self,
        async_client: AsyncDirectClient,
        httpx_mock: HTTPXMock,
        timing_data_keyframe: str,
    ) -> None:
        httpx_mock.add_response(text=timing_data_keyframe)

        _ = await async_client.fetch(
            year=2023, meeting="Pre-Season", file="TimingData.jsonStream"
        )

        requests = httpx_mock.get_requests()
        assert len(requests) == 1
        assert "TimingData.jsonStream" in str(requests[0].url)

    # ── Error paths ───────────────────────────────────────────────

    async def test_raises_on_404(
        self,
        async_client: AsyncDirectClient,
        httpx_mock: HTTPXMock,
    ) -> None:
        httpx_mock.add_response(status_code=404)
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            _ = await async_client.fetch(year=2023, file="NonExistent.json")
        assert exc_info.value.response.status_code == 404

    async def test_invalid_year_raises_without_http(
        self,
        async_client: AsyncDirectClient,
        httpx_mock: HTTPXMock,
    ) -> None:
        with pytest.raises(ValueError):
            _ = await async_client.fetch(year=2010)
        assert len(httpx_mock.get_requests()) == 0

    # ── Retry behavior ────────────────────────────────────────────

    async def test_retries_on_500_then_succeeds(
        self,
        async_client: AsyncDirectClient,
        httpx_mock: HTTPXMock,
        season_index: str,
    ) -> None:
        httpx_mock.add_response(status_code=500)
        httpx_mock.add_response(text=season_index)

        result = await async_client.fetch(year=2023)
        assert isinstance(result, list)
        assert len(httpx_mock.get_requests()) == 2

    async def test_does_not_retry_on_404(
        self,
        async_client: AsyncDirectClient,
        httpx_mock: HTTPXMock,
    ) -> None:
        httpx_mock.add_response(status_code=404)
        with pytest.raises(httpx.HTTPStatusError):
            _ = await async_client.fetch(year=2023)
        assert len(httpx_mock.get_requests()) == 1

    async def test_raises_after_max_retries(
        self,
        async_client: AsyncDirectClient,
        httpx_mock: HTTPXMock,
    ) -> None:
        httpx_mock.add_response(status_code=500, is_reusable=True)
        with pytest.raises(httpx.HTTPStatusError):
            _ = await async_client.fetch(year=2023)
        assert len(httpx_mock.get_requests()) == 3

    async def test_retries_on_timeout(
        self,
        async_client: AsyncDirectClient,
        httpx_mock: HTTPXMock,
        season_index: str,
    ) -> None:
        httpx_mock.add_exception(httpx.ReadTimeout("timed out"))
        httpx_mock.add_response(text=season_index)

        result = await async_client.fetch(year=2023)
        assert isinstance(result, list)
        assert len(httpx_mock.get_requests()) == 2


class TestFetchOne:
    async def test_returns_single_dict(
        self,
        async_client: AsyncDirectClient,
        httpx_mock: HTTPXMock,
        season_index: str,
    ) -> None:
        httpx_mock.add_response(text=season_index)
        result = await async_client.fetch_one(year=2023)
        assert isinstance(result, dict)


class TestGetSeason:
    """AsyncDirectClient.get_season() — fetching and caching."""

    async def test_returns_season(
        self,
        async_client: AsyncDirectClient,
        httpx_mock: HTTPXMock,
        season_index: str,
    ) -> None:
        httpx_mock.add_response(text=season_index)
        season = await async_client.get_season(year=2023)
        assert season is not None
        assert season.keyframe.year == 2023

    async def test_caches_after_first_call(
        self,
        async_client: AsyncDirectClient,
        httpx_mock: HTTPXMock,
        season_index: str,
    ) -> None:
        httpx_mock.add_response(text=season_index)
        _ = await async_client.get_season(year=2023)
        _ = await async_client.get_season(year=2023)
        assert len(httpx_mock.get_requests()) == 1

    async def test_clear_cache_forces_refetch(
        self,
        async_client: AsyncDirectClient,
        httpx_mock: HTTPXMock,
        season_index: str,
    ) -> None:
        httpx_mock.add_response(text=season_index, is_reusable=True)
        _ = await async_client.get_season(year=2023)
        async_client.clear_cache()
        _ = await async_client.get_season(year=2023)
        assert len(httpx_mock.get_requests()) == 2


class TestGetMeeting:
    async def test_returns_meeting(
        self,
        async_client: AsyncDirectClient,
        httpx_mock: HTTPXMock,
        season_index: str,
    ) -> None:
        httpx_mock.add_response(text=season_index)
        meeting = await async_client.get_meeting(year=2023, meeting="Pre-Season")
        assert meeting.folder_name is not None

    async def test_no_match_raises(
        self,
        async_client: AsyncDirectClient,
        httpx_mock: HTTPXMock,
        season_index: str,
    ) -> None:
        httpx_mock.add_response(text=season_index)
        with pytest.raises(ValueError, match=r"[Nn]o meeting"):
            _ = await async_client.get_meeting(year=2023, meeting="Nonexistent")


class TestGet:
    """AsyncDirectClient.get() — TaskGroup parallel fetch + model validation.

    Note: These tests require real model classes with KEYFRAME_FILE and
    STREAM_FILE set. Adjust the model imports and mock URLs to match your
    actual feed models. Skipped by default until you wire up concrete models.
    """

    async def test_fetches_keyframe_and_stream(
        self,
        async_client: AsyncDirectClient,
        httpx_mock: HTTPXMock,
        season_index: str,
        timing_data_keyframe: str,
        timing_data_stream: str,
    ) -> None:
        from pitwall.api_handler.models.timing_data import TimingDataF1

        # Season resolution
        httpx_mock.add_response(text=season_index)
        httpx_mock.add_response(text=timing_data_keyframe)
        httpx_mock.add_response(text=timing_data_stream)

        result = await async_client.get(
            model=TimingDataF1,
            year=2023,
            meeting="Pre-Season",
            session="Practice 1",
        )
        assert result is not None

    async def test_stream_failure_propagates(
        self,
        async_client: AsyncDirectClient,
        httpx_mock: HTTPXMock,
    ) -> None:
        """If one of the parallel fetches fails, the error should propagate.
        Testing with raw fetch calls to avoid model dependency."""
        httpx_mock.add_exception(
            httpx.HTTPStatusError(
                "error",
                request=httpx.Request("GET", f"{BASE}/2023/test"),
                response=httpx.Response(500),
            ),
            is_reusable=True,
        )

        with pytest.raises((httpx.HTTPStatusError, ExceptionGroup)):
            _ = await async_client.fetch(year=2023, file="NonExistent.json")


class TestContextManager:
    async def test_async_with_works(self, settings: ClientSettings) -> None:
        async with AsyncDirectClient(settings=settings) as client:
            assert client is not None

    async def test_default_settings(self) -> None:
        client = AsyncDirectClient()
        assert client._settings is not None # pyright: ignore[reportPrivateUsage]
        await client._client.aclose() # pyright: ignore[reportPrivateUsage]
