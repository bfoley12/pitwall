# tests/test_client_sync.py
"""Tests for DirectClient — synchronous HTTP client."""

import contextlib

import httpx
import pytest
from pytest_httpx import HTTPXMock

from pitwall.api_handler.client import DirectClient
from pitwall.api_handler.settings import ClientSettings

BASE = "https://livetiming.formula1.com/static"


class TestFetch:
    """DirectClient.fetch() — URL dispatch, response parsing, error paths."""

    # ── Happy paths ───────────────────────────────────────────────

    def test_no_args_fetches_root_index(
        self,
        sync_client: DirectClient,
        httpx_mock: HTTPXMock,
        year_index: str,
    ) -> None:
        httpx_mock.add_response(text=year_index)
        result = _ = sync_client.fetch()

        request = httpx_mock.get_request()
        assert request is not None
        assert str(request.url) == f"{BASE}/Index.json"
        assert isinstance(result, list)

    def test_year_fetches_season_index(
        self,
        sync_client: DirectClient,
        httpx_mock: HTTPXMock,
        season_index: str,
    ) -> None:
        httpx_mock.add_response(text=season_index)
        result = _ = sync_client.fetch(year=2023)

        request = httpx_mock.get_request()
        assert request is not None
        assert str(request.url) == f"{BASE}/2023/Index.json"
        assert len(result) > 0

    # ── Error paths ───────────────────────────────────────────────

    def test_raises_on_404(
        self,
        sync_client: DirectClient,
        httpx_mock: HTTPXMock,
    ) -> None:
        httpx_mock.add_response(status_code=404)
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            _ = sync_client.fetch(year=2023, file="NonExistent.json")
        assert exc_info.value.response.status_code == 404

    def test_raises_on_403(
        self,
        sync_client: DirectClient,
        httpx_mock: HTTPXMock,
    ) -> None:
        httpx_mock.add_response(status_code=403)
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            _ = sync_client.fetch(year=2023)
        assert exc_info.value.response.status_code == 403

    def test_invalid_year_raises_without_http(
        self,
        sync_client: DirectClient,
        httpx_mock: HTTPXMock,
    ) -> None:
        """Year validation should fire before any HTTP request."""
        with pytest.raises(ValueError):
            _ = sync_client.fetch(year=2010)
        assert len(httpx_mock.get_requests()) == 0

    # ── Retry behavior ────────────────────────────────────────────

    def test_retries_on_500_then_succeeds(
        self,
        sync_client: DirectClient,
        httpx_mock: HTTPXMock,
        season_index: str,
    ) -> None:
        httpx_mock.add_response(status_code=500)
        httpx_mock.add_response(text=season_index)

        result = _ = sync_client.fetch(year=2023)
        assert isinstance(result, list)
        assert len(httpx_mock.get_requests()) == 2

    def test_does_not_retry_on_404(
        self,
        sync_client: DirectClient,
        httpx_mock: HTTPXMock,
    ) -> None:
        httpx_mock.add_response(status_code=404)
        with pytest.raises(httpx.HTTPStatusError):
            _ = sync_client.fetch(year=2023)
        assert len(httpx_mock.get_requests()) == 1

    def test_raises_after_max_retries(
        self,
        sync_client: DirectClient,
        httpx_mock: HTTPXMock,
    ) -> None:
        httpx_mock.add_response(status_code=500, is_reusable=True)
        with pytest.raises(httpx.HTTPStatusError):
            _ = sync_client.fetch(year=2023)
        # 3 attempts total (initial + 2 retries)
        assert len(httpx_mock.get_requests()) == 3

    def test_retries_on_timeout_then_succeeds(
        self,
        sync_client: DirectClient,
        httpx_mock: HTTPXMock,
        season_index: str,
    ) -> None:
        httpx_mock.add_exception(httpx.ReadTimeout("timed out"))
        httpx_mock.add_response(text=season_index)

        result = _ = sync_client.fetch(year=2023)
        assert isinstance(result, list)
        assert len(httpx_mock.get_requests()) == 2

    def test_raises_timeout_after_max_retries(
        self,
        sync_client: DirectClient,
        httpx_mock: HTTPXMock,
    ) -> None:
        httpx_mock.add_exception(httpx.ReadTimeout("timed out"), is_reusable=True)
        with pytest.raises(httpx.ReadTimeout):
            _ = sync_client.fetch(year=2023)
        assert len(httpx_mock.get_requests()) == 3


class TestFetchOne:
    """DirectClient.fetch_one() — unwraps the first element."""

    def test_returns_single_dict(
        self,
        sync_client: DirectClient,
        httpx_mock: HTTPXMock,
        season_index: str,
    ) -> None:
        httpx_mock.add_response(text=season_index)
        result = _ = sync_client.fetch_one(year=2023)
        assert isinstance(result, dict)
        # fetch_one should unwrap the list from fetch()
        assert "Year" in result or "Years" in result


class TestGetSeason:
    """DirectClient.get_season() — fetching and caching."""

    def test_returns_season(
        self,
        sync_client: DirectClient,
        httpx_mock: HTTPXMock,
        season_index: str,
    ) -> None:
        httpx_mock.add_response(text=season_index)
        season = sync_client.get_season(year=2023)
        assert season.keyframe.year == 2023

    def test_caches_after_first_call(
        self,
        sync_client: DirectClient,
        httpx_mock: HTTPXMock,
        season_index: str,
    ) -> None:
        httpx_mock.add_response(text=season_index)
        _ = sync_client.get_season(year=2023)
        _ = sync_client.get_season(year=2023)
        # Only one HTTP request — second call used cache
        assert len(httpx_mock.get_requests()) == 1

    def test_clear_cache_forces_refetch(
        self,
        sync_client: DirectClient,
        httpx_mock: HTTPXMock,
        season_index: str,
    ) -> None:
        httpx_mock.add_response(text=season_index, is_reusable=True)
        _ = sync_client.get_season(year=2023)
        sync_client.clear_cache()
        _ = sync_client.get_season(year=2023)
        assert len(httpx_mock.get_requests()) == 2

    def test_different_years_are_cached_independently(
        self,
        sync_client: DirectClient,
        httpx_mock: HTTPXMock,
        season_index: str,
    ) -> None:
        # This will fail validation for the second year since the JSON
        # has Year:2023, but the point is testing cache key separation.
        httpx_mock.add_response(text=season_index, is_reusable=True)
        _ = sync_client.get_season(year=2023)
        with contextlib.suppress(Exception):
            _ = sync_client.get_season(year=2024)
        assert len(httpx_mock.get_requests()) == 2


class TestGetMeeting:
    """DirectClient.get_meeting() — delegates to season."""

    def test_returns_meeting(
        self,
        sync_client: DirectClient,
        httpx_mock: HTTPXMock,
        season_index: str,
    ) -> None:
        httpx_mock.add_response(text=season_index)
        meeting = sync_client.get_meeting(year=2023, meeting="Pre-Season")
        assert meeting.folder_name is not None

    def test_ambiguous_match_raises(
        self,
        sync_client: DirectClient,
        httpx_mock: HTTPXMock,
        season_index: str,
    ) -> None:
        httpx_mock.add_response(text=season_index)
        # "Grand Prix" should match multiple meetings
        with pytest.raises(ValueError, match=r"[Aa]mbiguous"):
            _ = sync_client.get_meeting(year=2023, meeting="Grand Prix")

    def test_no_match_raises(
        self,
        sync_client: DirectClient,
        httpx_mock: HTTPXMock,
        season_index: str,
    ) -> None:
        httpx_mock.add_response(text=season_index)
        with pytest.raises(ValueError, match=r"[Nn]o meeting"):
            _ = sync_client.get_meeting(year=2023, meeting="Nonexistent Circuit")


class TestContextManager:
    """DirectClient context manager and lifecycle."""

    def test_with_statement_works(self, settings: ClientSettings) -> None:
        with DirectClient(settings=settings) as client:
            assert client is not None

    def test_default_settings(self) -> None:
        client = DirectClient()
        assert client._settings is not None  # pyright: ignore[reportPrivateUsage]
        client._client.close()  # pyright: ignore[reportPrivateUsage]
