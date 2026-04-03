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
from pitwall.api_handler.models.session import Session, SessionSubType
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

    def get_models(self, explain: bool = False) -> list[str] | str:
        model_list = registry.get_names()
        result = ""
        if explain:
            for model in model_list:
                result += f"{model}: {registry.get(model).explain()}\n"
        else:
            result = model_list
        return result
    @staticmethod
    def _resolve_model(
        model: str | type[F1KeyframeContainer] | None,
        meeting: str | None,
        session: Session | str | SessionSubType | None,
    ) -> type[F1KeyframeContainer]:
        if model is None:
            if meeting is None and session is None:
                return Season
            elif meeting is None and isinstance(session, (str, SessionSubType)):
                raise ValueError("meeting must be specified if session is of type 'str' or 'SessionSubType'")
            else:
                raise ValueError("model must be specified if session is provided")
        if isinstance(model, str):
            return registry.get(model)
        return model


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
        *,
        session: Session,
        stream_only: bool = False,
    ) -> T: ...

    @overload
    async def get[T: F1KeyframeContainer](
        self,
        model: type[T],
        *,
        year: int,
        meeting: str | None = None,
        session: str | SessionSubType | None = None,
        stream_only: bool = False,
    ) -> T: ...

    @overload
    async def get(
        self,
        model: str,
        *,
        year: int,
        meeting: str | None = None,
        session: str | SessionSubType | None = None,
        stream_only: bool = False,
    ) -> F1KeyframeContainer: ...

    @overload
    async def get(
        self,
        model: None = None,
        *,
        year: int,
        meeting: str | None = None,
    ) -> F1KeyframeContainer: ...

    async def get(
        self,
        model: str | type[F1KeyframeContainer] | None = None,
        *,
        year: int | None = None,
        meeting: str | None = None,
        session: Session | str | SessionSubType | None = None,
        stream_only: bool = False,
    ) -> F1KeyframeContainer:
        resolved = self._resolve_model(model, meeting, session)

        if isinstance(session, Session):
            r_year, r_meeting, r_session = session.path_parts()
        else:
            if year is None:
                raise ValueError(
                    "year is required when session is not a Session object"
                )
            r_year = str(year)
            r_meeting = (
                (await self.get_meeting(year=year, meeting=meeting)).folder_name
                if meeting
                else meeting
            )
            if meeting is not None:
                r_session = (
                    await self._resolve_session_folder(year, meeting, session)
                    if session
                    else None
                )
            else:
                r_session = None

        tasks: dict[str, asyncio.Task[list[dict[str, JsonValue]]]] = {}
        async with asyncio.timeout(self._settings.request_timeout):
            async with asyncio.TaskGroup() as tg:
                if resolved.KEYFRAME_FILE is not None and not stream_only:
                    tasks["keyframe"] = tg.create_task(
                        self.fetch(
                            year=r_year,
                            meeting=r_meeting,
                            session=r_session,
                            file=resolved.KEYFRAME_FILE,
                        )
                    )
                if resolved.STREAM_FILE is not None:
                    tasks["stream"] = tg.create_task(
                        self.fetch(
                            year=r_year,
                            meeting=r_meeting,
                            session=r_session,
                            file=resolved.STREAM_FILE,
                        )
                    )
        raw = {k: t.result() for k, t in tasks.items()}
        return resolved.model_validate(raw)

    async def _resolve_session_folder(
        self,
        year: int,
        meeting: str,
        session: str | SessionSubType,
    ) -> str:
        session = SessionSubType.parse(session)
        full_meeting = await self.get_meeting(year=year, meeting=meeting)
        return full_meeting.get_session(session.value).folder_name

    async def get_season(self, year: int) -> Season | None:
        if year not in self._season_cache:
            try:
                data = await self.fetch_one(year=year)
                self._season_cache[year] = Season.model_validate(data)
            except httpx.HTTPStatusError as e:
                if e.response.status_code in (403, 404):
                    return None
                else:
                    raise
        return self._season_cache[year]

    async def get_available_seasons(self) -> dict[int, Season]:
        years = range(2018, date.today().year + 1)
        tasks: dict[int, asyncio.Task[Season | None]] = {}

        async with asyncio.timeout(self._settings.request_timeout):
            async with asyncio.TaskGroup() as tg:
                for year in years:
                    tasks[year] = tg.create_task(self.get_season(year))

        return {
            year: season
            for year, task in tasks.items()
            if (season := task.result()) is not None
        }

    async def get_meeting(self, year: int, meeting: str) -> Meeting:
        season = await self.get_season(year)
        if season is None:
            raise ValueError(f"Invalid year provided: {year}")
        return season.keyframe.get_meeting(meeting)

    @_retry_policy
    async def fetch(
        self,
        year: str | int | None = None,
        meeting: str | None = None,
        session: str | None = None,
        file: str = "Index.json",
    ) -> list[dict[str, JsonValue]]:
        url = self._build_url(
            year=None if year is None else int(year),
            meeting=meeting,
            session=session,
            file=file,
        )
        response = (await self._client.get(url)).raise_for_status()
        return self._decode_response(response, file)

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
        *,
        session: Session,
        stream_only: bool = False,
    ) -> T: ...

    @overload
    def get[T: F1KeyframeContainer](
        self,
        model: type[T],
        *,
        year: int,
        meeting: str | None = None,
        session: str | SessionSubType | None = None,
        stream_only: bool = False,
    ) -> T: ...

    @overload
    def get(
        self,
        model: str,
        *,
        year: int,
        meeting: str | None = None,
        session: str | SessionSubType | None = None,
        stream_only: bool = False,
    ) -> F1KeyframeContainer: ...

    @overload
    def get(
        self,
        model: None = None,
        *,
        year: int,
        meeting: str | None = None,
    ) -> F1KeyframeContainer: ...

    def get(
        self,
        model: str | type[F1KeyframeContainer] | None = None,
        *,
        year: int | None = None,
        meeting: str | None = None,
        session: Session | str | SessionSubType | None = None,
        stream_only: bool = False,
    ) -> F1KeyframeContainer:
        resolved = self._resolve_model(model, meeting, session)

        if isinstance(session, Session):
            r_year, r_meeting, r_session = session.path_parts()
        else:
            if year is None:
                raise ValueError(
                    "year is required when session is not a Session object"
                )
            r_year = str(year)
            r_meeting = (
                self.get_meeting(year=year, meeting=meeting).folder_name
                if meeting
                else meeting
            )
            if meeting is not None:
                r_session = (
                    self._resolve_session_folder(year, meeting, session)
                    if session
                    else None
                )
            else:
                r_session = None

        raw: dict[str, object] = {}
        if resolved.KEYFRAME_FILE is not None and not stream_only:
            raw["keyframe"] = self.fetch(
                year=r_year,
                meeting=r_meeting,
                session=r_session,
                file=resolved.KEYFRAME_FILE,
            )
        if resolved.STREAM_FILE is not None:
            raw["stream"] = self.fetch(
                year=r_year,
                meeting=r_meeting,
                session=r_session,
                file=resolved.STREAM_FILE,
            )
        return resolved.model_validate(raw)

    def _resolve_session_folder(
        self,
        year: int,
        meeting: str,
        session: str | SessionSubType,
    ) -> str:
        session = SessionSubType.parse(session)
        full_meeting = self.get_meeting(year=year, meeting=meeting)
        return full_meeting.get_session(session.value).folder_name

    def get_season(self, year: int) -> Season | None:
        if year not in self._season_cache:
            try:
                data = self.fetch_one(year=year)
                self._season_cache[year] = Season.model_validate(data)
            except httpx.HTTPStatusError as e:
                if e.response.status_code in (403, 404):
                    return None
                raise
        return self._season_cache[year]

    def get_available_seasons(self) -> dict[int, Season]:
        return {
            year: season
            for year in range(2018, date.today().year + 1)
            if (season := self.get_season(year)) is not None
        }

    def get_meeting(self, year: int, meeting: str) -> Meeting:
        season = self.get_season(year)
        if season is None:
            raise ValueError(f"Invalid year provided: {year}")
        return season.keyframe.get_meeting(meeting)

    @_retry_policy
    def fetch(
        self,
        year: str | int | None = None,
        meeting: str | None = None,
        session: str | None = None,
        file: str = "Index.json",
    ) -> list[dict[str, JsonValue]]:
        url = self._build_url(
            year=None if year is None else int(year),
            meeting=meeting,
            session=session,
            file=file,
        )
        response = self._client.get(url).raise_for_status()
        return self._decode_response(response, file)

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
