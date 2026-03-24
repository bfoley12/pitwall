import base64
import json
import zlib
from typing import Any

import httpx

from pitwall.api_handler.models.base import F1DataContainerT, F1Model, F1ModelT
from pitwall.api_handler.models.session import (
    SessionSubType,
)
from pitwall.api_handler.path_resolver import PathResolver

_RACE_SESSIONS: frozenset[SessionSubType] = frozenset(
    {
        SessionSubType.RACE,
        SessionSubType.SPRINT,
    }
)


class F1Client:
    def __init__(self) -> None:
        self.http: httpx.Client = httpx.Client()

    def get(
        self,
        model: type[F1DataContainerT],
        year: int,
        meeting: str | None = None,
        session: SessionSubType | None = None,
    ) -> F1DataContainerT:
        raw: dict[str, Any] = {}

        if model.KEYFRAME_FILE is not None:
            raw["keyframe"] = self._try_fetch_raw(
                year=year,
                meeting=meeting,
                session=session,
                file=model.KEYFRAME_FILE,
            )

        if model.STREAM_FILE is not None:
            raw["stream"] = self._try_fetch_raw(
                year=year,
                meeting=meeting,
                session=session,
                file=model.STREAM_FILE,
            )
        return model.model_validate(raw)

    def get_file(
        self, year: int, meeting: str, session: SessionSubType, file: str
    ) -> F1Model:
        return self.fetch(
            model=F1Model, year=year, meeting=meeting, session=session, file=file
        )

    def fetch(
        self,
        model: type[F1ModelT],
        year: int | None = None,
        meeting: str | None = None,
        session: SessionSubType | None = None,
        file: str = "Index.json",
    ) -> F1ModelT:
        url = PathResolver(year=year, meeting=meeting, session=session, file=file).url
        response = self.http.get(url)
        _ = response.raise_for_status()
        data = self._decode_response(response, file)
        return model.model_validate(data)

    def _fetch_raw(
        self,
        year: int | None = None,
        meeting: str | None = None,
        session: SessionSubType | None = None,
        file: str = "Index.json",
    ) -> object:
        url = PathResolver(year=year, meeting=meeting, session=session, file=file).url
        response = self.http.get(url)
        _ = response.raise_for_status()
        return self._decode_response(response, file)

    def _try_fetch_raw(
        self,
        year: int | None = None,
        meeting: str | None = None,
        session: SessionSubType | None = None,
        file: str = "Index.json",
    ) -> object | None:
        try:
            return self._fetch_raw(
                year=year, meeting=meeting, session=session, file=file
            )
        except httpx.HTTPStatusError:
            return None

    def _decompress(self, blob: str) -> Any:
        decoded = base64.b64decode(blob + "==")
        decompressed = zlib.decompress(decoded, -zlib.MAX_WBITS)
        return json.loads(decompressed)

    def _decode_response(
        self, response: httpx.Response, file: str
    ) -> list[dict[str, Any]]:
        text = response.text.lstrip("\ufeff")

        if file.endswith(".z.json"):
            return [self._decompress(json.loads(text))]

        if file.endswith(".json"):
            return [json.loads(text)]

        compressed = file.endswith(".z.jsonStream")
        entries: list[dict[str, Any]] = []

        for line in text.strip().split("\n"):
            if not line:
                continue

            if compressed:
                quote_idx = line.index('"')
                timestamp = line[:quote_idx]
                parsed = self._decompress(line[quote_idx:].strip('"'))
                key = "Entries" if "Entries" in parsed else "Position"
                for payload in parsed[key]:
                    entries.append({"Timestamp": timestamp, "Data": payload})
            else:
                brace_idx = line.index("{")
                timestamp = line[:brace_idx]
                payload = json.loads(line[brace_idx:])
                entries.append({"Timestamp": timestamp, "Data": payload})

        return entries
