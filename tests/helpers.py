# tests/test_helpers.py
"""Tests for pure functions in client.py — no HTTP mocking needed."""

import base64
import json
import zlib
from datetime import date

import httpx
import pytest

from pitwall.api_handler.client import (
    _BaseClient,
    _decompress,
    _is_retryable,
    _validate_year,
)
from pitwall.api_handler.settings import ClientSettings

# ── _validate_year ────────────────────────────────────────────────────


class TestValidateYear:
    def test_accepts_current_year(self) -> None:
        _validate_year(date.today().year)  # should not raise

    def test_accepts_2018(self) -> None:
        _validate_year(2018)

    def test_rejects_2017(self) -> None:
        with pytest.raises(ValueError, match="2018"):
            _validate_year(2017)

    def test_rejects_future_year(self) -> None:
        with pytest.raises(ValueError):
            _validate_year(date.today().year + 1)

    def test_rejects_negative(self) -> None:
        with pytest.raises(ValueError):
            _validate_year(-1)


# ── _is_retryable ────────────────────────────────────────────────────


class TestIsRetryable:
    @staticmethod
    def _make_status_error(code: int) -> httpx.HTTPStatusError:
        return httpx.HTTPStatusError(
            "error",
            request=httpx.Request("GET", "https://example.com"),
            response=httpx.Response(code),
        )

    @pytest.mark.parametrize("code", [500, 502, 503, 504])
    def test_retries_5xx(self, code: int) -> None:
        assert _is_retryable(self._make_status_error(code)) is True

    @pytest.mark.parametrize("code", [400, 401, 403, 404, 422])
    def test_does_not_retry_4xx(self, code: int) -> None:
        assert _is_retryable(self._make_status_error(code)) is False

    def test_retries_timeout(self) -> None:
        exc = httpx.ReadTimeout("timed out")
        assert _is_retryable(exc) is True

    def test_retries_connect_timeout(self) -> None:
        exc = httpx.ConnectTimeout("connect timed out")
        assert _is_retryable(exc) is True

    def test_does_not_retry_generic_exception(self) -> None:
        assert _is_retryable(ValueError("nope")) is False


# ── _decompress ───────────────────────────────────────────────────────


class TestDecompress:
    @staticmethod
    def _compress(data: dict) -> str:
        """Create a base64-encoded zlib-compressed blob (raw deflate)."""
        raw = json.dumps(data).encode()
        compressed = zlib.compress(raw, level=6)[2:-4]  # strip zlib header/checksum
        return base64.b64encode(compressed).decode()

    def test_round_trips(self) -> None:
        original = {"key": "value", "nested": {"a": 1}}
        blob = self._compress(original)
        result = _decompress(blob)
        assert result == original

    def test_handles_missing_padding(self) -> None:
        original = {"test": True}
        blob = self._compress(original).rstrip("=")
        result = _decompress(blob)
        assert result == original


# ── _build_url ────────────────────────────────────────────────────────


class TestBuildUrl:
    @pytest.fixture
    def client(self) -> _BaseClient:
        return _BaseClient(settings=ClientSettings())

    BASE = "https://livetiming.formula1.com/static"

    def test_no_args(self, client: _BaseClient) -> None:
        assert client._build_url() == f"{self.BASE}/Index.json"

    def test_year_only(self, client: _BaseClient) -> None:
        assert client._build_url(year=2023) == f"{self.BASE}/2023/Index.json"

    def test_year_and_meeting(self, client: _BaseClient) -> None:
        url = client._build_url(year=2023, meeting="2023-03-05_Bahrain")
        assert url == f"{self.BASE}/2023/2023-03-05_Bahrain/Index.json"

    def test_full_path(self, client: _BaseClient) -> None:
        url = client._build_url(
            year=2023,
            meeting="2023-03-05_Bahrain",
            session="2023-03-04_Practice_1",
            file="TimingData.jsonStream",
        )
        assert url == (
            f"{self.BASE}/2023/2023-03-05_Bahrain/"
            "2023-03-04_Practice_1/TimingData.jsonStream"
        )

    def test_custom_file(self, client: _BaseClient) -> None:
        url = client._build_url(year=2023, file="Index.json")
        assert url == f"{self.BASE}/2023/Index.json"

    def test_meeting_without_year_raises(self, client: _BaseClient) -> None:
        with pytest.raises(ValueError, match="year is required"):
            client._build_url(meeting="2023-03-05_Bahrain")

    def test_session_without_meeting_raises(self, client: _BaseClient) -> None:
        with pytest.raises(ValueError, match="meeting is required"):
            client._build_url(year=2023, session="2023-03-04_Practice_1")

    def test_invalid_year_raises(self, client: _BaseClient) -> None:
        with pytest.raises(ValueError):
            client._build_url(year=1990)


# ── _decode_response ──────────────────────────────────────────────────


class TestDecodeResponse:
    @staticmethod
    def _make_response(text: str) -> httpx.Response:
        return httpx.Response(200, text=text)

    def test_json_returns_single_item_list(self) -> None:
        payload = {"Year": 2023, "Meetings": []}
        response = self._make_response(json.dumps(payload))
        result = _BaseClient._decode_response(response, "Index.json")
        assert result == [payload]

    def test_json_strips_bom(self) -> None:
        payload = {"key": "value"}
        response = self._make_response("\ufeff" + json.dumps(payload))
        result = _BaseClient._decode_response(response, "Index.json")
        assert result == [payload]

    def test_json_stream_parses_timestamped_lines(self) -> None:
        lines = (
            '00:00:01.000{"Utc":"2023-01-01T00:00:00Z","Value":1}\r\n'
            '00:05:30.500{"Utc":"2023-01-01T00:05:00Z","Value":2}\r\n'
        )
        response = self._make_response(lines)
        result = _BaseClient._decode_response(response, "TimingData.jsonStream")

        assert len(result) == 2
        assert result[0]["Timestamp"] == "00:00:01.000"
        assert result[1]["Timestamp"] == "00:05:30.500"
        # Data should be the parsed JSON payload
        assert result[0]["Data"]["Value"] == 1
        assert result[1]["Data"]["Value"] == 2

    def test_json_stream_strips_bom(self) -> None:
        lines = '\ufeff00:00:01.000{"key":"val"}\r\n'
        response = self._make_response(lines)
        result = _BaseClient._decode_response(response, "TimingData.jsonStream")
        assert len(result) == 1

    def test_json_stream_skips_empty_lines(self) -> None:
        lines = '00:00:01.000{"a":1}\r\n\r\n00:00:02.000{"b":2}\r\n'
        response = self._make_response(lines)
        result = _BaseClient._decode_response(response, "Data.jsonStream")
        assert len(result) == 2

    def test_json_stream_malformed_no_brace_raises(self) -> None:
        response = self._make_response("00:00:01.000 no json here\r\n")
        with pytest.raises(ValueError, match="no brace found"):
            _BaseClient._decode_response(response, "Data.jsonStream")

    def test_z_json_decompresses_single_blob(self) -> None:
        """Test .z.json: base64-encoded zlib blob wrapping a JSON dict."""
        payload = {"Entries": [{"Utc": "2023-01-01T00:00:00Z"}]}
        raw = json.dumps(payload).encode()
        compressed = zlib.compress(raw, level=6)[2:-4]
        blob = base64.b64encode(compressed).decode()
        # The API returns the blob as a JSON string
        response = self._make_response(json.dumps(blob))
        result = _BaseClient._decode_response(response, "CarData.z.json")
        assert result == [payload]

    def test_z_json_stream_decompresses_entries(self) -> None:
        """Test .z.jsonStream: each line is timestamp + base64 blob."""
        payload = {
            "Entries": [
                {"Utc": "2023-01-01T00:00:00Z", "Cars": {}},
                {"Utc": "2023-01-01T00:00:01Z", "Cars": {}},
            ]
        }
        raw = json.dumps(payload).encode()
        compressed = zlib.compress(raw, level=6)[2:-4]
        blob = base64.b64encode(compressed).decode()
        line = f'00:00:01.000"{blob}"\r\n'
        response = self._make_response(line)
        result = _BaseClient._decode_response(response, "CarData.z.jsonStream")
        # Each Entry becomes a row with Timestamp + Data
        assert len(result) == 2
        assert result[0]["Timestamp"] == "00:00:01.000"

    def test_z_json_stream_handles_position_key(self) -> None:
        """Position.z.jsonStream uses 'Position' instead of 'Entries'."""
        payload = {
            "Position": [
                {"Timestamp": "2023-01-01T00:00:00Z", "Entries": {}},
            ]
        }
        raw = json.dumps(payload).encode()
        compressed = zlib.compress(raw, level=6)[2:-4]
        blob = base64.b64encode(compressed).decode()
        line = f'00:00:01.000"{blob}"\r\n'
        response = self._make_response(line)
        result = _BaseClient._decode_response(response, "Position.z.jsonStream")
        assert len(result) == 1

    def test_z_json_stream_malformed_no_quote_raises(self) -> None:
        response = self._make_response("00:00:01.000 no quote here\r\n")
        with pytest.raises(ValueError, match="no quote found"):
            _BaseClient._decode_response(response, "Data.z.jsonStream")
