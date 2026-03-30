import asyncio
import base64
import json
import zlib
from datetime import date
from typing import Final, cast, overload

import httpx
from pydantic import JsonValue
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential_jitter,
)

import pitwall.api_handler.registry as registry
from pitwall.api_handler.models.base import F1KeyframeContainer
from pitwall.api_handler.models.meeting import Meeting
from pitwall.api_handler.models.season import Season
from pitwall.api_handler.models.session import SessionSubType
from pitwall.api_handler.settings import ClientSettings

_SESSION_ALIASES: dict[str, str] = {
    "practice_1": "Practice_1",
    "fp1": "Practice_1",
    "free_practice_1": "Practice_1",
    "practice_2": "Practice_2",
    "fp2": "Practice_2",
    "free_practice_2": "Practice_2",
    "practice_3": "Practice_3",
    "fp3": "Practice_3",
    "free_practice_3": "Practice_3",
    "qualifying": "Qualifying",
    "q": "Qualifying",
    "sprint_qualifying": "Sprint_Qualifying",
    "sq": "Sprint_Qualifying",
    "sprint_shootout": "Sprint_Qualifying",
    "sprint": "Sprint",
    "sprint_race": "Sprint",
    "sr": "Sprint",
    "race": "Race",
    "r": "Race",
}


def _validate_year(year: int) -> None:
    start = 2018
    end = date.today().year
    if not start <= year <= end:
        raise ValueError(f"Year must be between {start} and {end}. Got {year}")


def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, httpx.TimeoutException):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code >= 500
    return False


_retry_policy = retry(
    reraise=True,
    wait=wait_exponential_jitter(initial=1, max=10, jitter=0.5),
    retry=retry_if_exception(_is_retryable),
    stop=stop_after_attempt(3),
)


def _decompress(blob: str) -> dict[str, JsonValue]:
    # Only pad if padding is missing from end of blob
    decoded = base64.b64decode(blob + "=" * (-len(blob) % 4))
    decompressed = zlib.decompress(decoded, -zlib.MAX_WBITS)
    return cast(dict[str, JsonValue], json.loads(decompressed))


class _BaseClient:
    _BASE_URL: Final[str] = "https://livetiming.formula1.com/static"

    def __init__(self, *, settings: ClientSettings | None = None) -> None:
        if settings is None:
            settings = ClientSettings()

        self._settings: ClientSettings = settings
        self._TIMEOUT: httpx.Timeout = httpx.Timeout(
            timeout=settings.timeout, connect=settings.connect, read=settings.read
        )
        self._LIMITS: httpx.Limits = httpx.Limits(
            max_connections=settings.max_connections,
            max_keepalive_connections=settings.max_keepalive_connections,
        )
        self._FOLLOW_REDIRECTS: bool = settings.follow_redirects
        self._season_cache: dict[int, Season] = {}

    def clear_cache(self) -> None:
        self._season_cache.clear()

    def _build_url(
        self,
        year: int | None = None,
        meeting: str | None = None,
        session: str | None = None,
        file: str = "Index.json",
    ) -> str:
        parts = [self._BASE_URL]

        if year is not None:
            _validate_year(year)
            parts.append(str(year))

        if meeting is not None:
            if year is None:
                raise ValueError("year is required when specifying meeting")

            parts.append(meeting)

        if session is not None:
            if meeting is None:
                raise ValueError("meeting is required when specifying session")
            parts.append(session)

        parts.append(file)
        return "/".join(parts)

    @staticmethod
    def _decode_response(
        response: httpx.Response, file: str
    ) -> list[dict[str, JsonValue]]:
        text = response.text.lstrip("\ufeff")

        if file.endswith(".z.json"):
            blob = cast(str, json.loads(text))
            return [_decompress(blob)]

        if file.endswith(".json"):
            return [cast(dict[str, JsonValue], json.loads(text))]

        compressed = file.endswith(".z.jsonStream")
        entries: list[dict[str, JsonValue]] = []

        for line in text.strip().split("\n"):
            if not line:
                continue

            if compressed:
                try:
                    quote_idx = line.index('"')
                except ValueError as err:
                    raise ValueError(
                        f"Malformed z.jsonStream line (no quote found): {line[:80]!r}"
                    ) from err
                timestamp = line[:quote_idx]
                parsed = _decompress(line[quote_idx:].strip('"'))
                # TODO: Find better way to address compressed files containing lists
                key = "Entries" if "Entries" in parsed else "Position"
                value = parsed.get(key)
                if not isinstance(value, list):
                    continue
                for payload in value:
                    entries.append({"Timestamp": timestamp, "Data": payload})
            else:
                try:
                    brace_idx = line.index("{")
                except ValueError as err:
                    raise ValueError(
                        f"Malformed z.jsonStream line (no brace found): {line[:80]!r}"
                    ) from err
                timestamp = line[:brace_idx]
                payload = cast(JsonValue, json.loads(line[brace_idx:]))
                entries.append({"Timestamp": timestamp, "Data": payload})

        return entries


class AsyncDirectClient(_BaseClient):
    def __init__(self, *, settings: ClientSettings | None = None) -> None:
        if settings is None:
            settings = ClientSettings()
        super().__init__(settings=settings)
        self._client: httpx.AsyncClient = httpx.AsyncClient(
            timeout=self._TIMEOUT,
            limits=self._LIMITS,
            follow_redirects=self._FOLLOW_REDIRECTS,
        )

    async def __aenter__(self) -> AsyncDirectClient:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self._client.aclose()

    @overload
    async def get[T: F1KeyframeContainer](
        self,
        model: type[T],
        year: int,
        meeting: str | None = None,
        session: str | None = None,
    ) -> T: ...

    @overload
    async def get(
        self,
        model: str,
        year: int,
        meeting: str | None = None,
        session: str | None = None,
    ) -> F1KeyframeContainer: ...

    async def get(
        self,
        model: str | type[F1KeyframeContainer],
        year: int,
        meeting: str | None = None,
        session: str | None = None,
    ) -> F1KeyframeContainer:
        resolved = registry.get(model) if isinstance(model, str) else model

        tasks: dict[str, asyncio.Task[list[dict[str, JsonValue]]]] = {}
        async with asyncio.timeout(self._settings.request_timeout):
            async with asyncio.TaskGroup() as tg:
                if resolved.KEYFRAME_FILE is not None:
                    tasks["keyframe"] = tg.create_task(
                        self.fetch(
                            year=year,
                            meeting=meeting,
                            session=session,
                            file=resolved.KEYFRAME_FILE,
                        )
                    )
                if resolved.STREAM_FILE is not None:
                    tasks["stream"] = tg.create_task(
                        self.fetch(
                            year=year,
                            meeting=meeting,
                            session=session,
                            file=resolved.STREAM_FILE,
                        )
                    )
        raw = {k: t.result() for k, t in tasks.items()}
        return resolved.model_validate(raw)

    async def get_season(self, year: int) -> Season:
        if year not in self._season_cache:
            data = await self.fetch_one(year=year)
            self._season_cache[year] = Season.model_validate(data)
        return self._season_cache[year]

    async def get_meeting(self, year: int, meeting: str) -> Meeting:
        season = await self.get_season(year)
        return season.keyframe.get_meeting(meeting)

    @_retry_policy
    async def fetch(
        self,
        year: int | None = None,
        meeting: str | None = None,
        session: str | None = None,
        file: str = "Index.json",
    ) -> list[dict[str, JsonValue]]:
        if meeting is not None and year is not None:
            meeting = (await self.get_meeting(year=year, meeting=meeting)).folder_name
        url = self._build_url(year=year, meeting=meeting, session=session, file=file)
        response = (await self._client.get(url)).raise_for_status()
        return self._decode_response(response, file)

    # Unused but left for convenience of users
    async def fetch_one(
        self,
        year: int | None = None,
        meeting: str | None = None,
        session: str | None = None,
        file: str = "Index.json",
    ) -> dict[str, JsonValue]:
        return (
            await self.fetch(year=year, meeting=meeting, session=session, file=file)
        )[0]


class DirectClient(_BaseClient):
    def __init__(self, *, settings: ClientSettings | None = None) -> None:
        if settings is None:
            settings = ClientSettings()
        super().__init__(settings=settings)
        self._client: httpx.Client = httpx.Client(
            timeout=self._TIMEOUT,
            limits=self._LIMITS,
            follow_redirects=self._FOLLOW_REDIRECTS,
        )

    def __enter__(self) -> DirectClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self._client.close()

    @overload
    def get[T: F1KeyframeContainer](
        self,
        model: type[T],
        year: int,
        meeting: str | None = None,
        session: str | None = None,
    ) -> T: ...

    @overload
    def get(
        self,
        model: str,
        year: int,
        meeting: str | None = None,
        session: str | None = None,
    ) -> F1KeyframeContainer: ...

    def get(
        self,
        model: str | type[F1KeyframeContainer],
        year: int,
        meeting: str | None = None,
        session: str | SessionSubType | None = None,
    ) -> F1KeyframeContainer:
        resolved = registry.get(model) if isinstance(model, str) else model
        if isinstance(session, str):
            session = SessionSubType.parse(session)

        raw: dict[str, object] = {}
        if resolved.KEYFRAME_FILE is not None:
            raw["keyframe"] = self.fetch(
                year=year,
                meeting=meeting,
                session=session,
                file=resolved.KEYFRAME_FILE,
            )
        if resolved.STREAM_FILE is not None:
            raw["stream"] = self.fetch(
                year=year,
                meeting=meeting,
                session=session,
                file=resolved.STREAM_FILE,
            )
        return resolved.model_validate(raw)

    def get_season(self, year: int) -> Season:
        if year not in self._season_cache:
            data = self.fetch_one(year=year)
            self._season_cache[year] = Season.model_validate(data)
        return self._season_cache[year]

    def get_meeting(self, year: int, meeting: str) -> Meeting:
        season = self.get_season(year)
        return season.keyframe.get_meeting(meeting)

    @_retry_policy
    def fetch(
        self,
        year: int | None = None,
        meeting: str | None = None,
        session: SessionSubType | None = None,
        file: str = "Index.json",
    ) -> list[dict[str, JsonValue]]:
        if meeting is not None and year is not None:
            full_meeting = self.get_meeting(year=year, meeting=meeting)
            meeting = full_meeting.folder_name
            if session is not None:
                session = full_meeting.get_session(session.value).folder_name  # pyright: ignore[reportAssignmentType]
        url = self._build_url(year=year, meeting=meeting, session=session, file=file)
        response = self._client.get(url).raise_for_status()
        return self._decode_response(response, file)

    # Unused but left for convenience of users
    def fetch_one(
        self,
        year: int | None = None,
        meeting: str | None = None,
        session: SessionSubType | None = None,
        file: str = "Index.json",
    ) -> dict[str, JsonValue]:
        return self.fetch(year=year, meeting=meeting, session=session, file=file)[0]


__all__ = [
    "AsyncDirectClient",
    "DirectClient",
]
