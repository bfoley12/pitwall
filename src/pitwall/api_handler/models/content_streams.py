from typing import Any, ClassVar

import polars as pl
from pydantic import JsonValue, model_validator

from .base import F1DataContainer, F1Model, F1Stream


class ContentStream(F1Model):
    type: str
    name: str
    language: str
    uri: str
    path: str | None = None
    utc: str | None = None


class ContentStreamsKeyframe(F1Model):
    streams: list[ContentStream]

    @model_validator(mode="before")
    @classmethod
    def _unwrap(cls, data: dict[str, JsonValue]) -> dict[str, JsonValue]:
        if "Streams" in data:
            return {"streams": data["Streams"]}
        return data


class ContentStreamsStream(F1Stream):
    SCHEMA: ClassVar[dict[str, pl.DataType]] = {
        "timestamp": pl.Duration("ms"),
        "type": pl.Utf8(),
        "name": pl.Utf8(),
        "language": pl.Utf8(),
        "uri": pl.Utf8(),
        "path": pl.Utf8(),
        "utc": pl.Utf8(),
    }

    @classmethod
    def _extract_rows(
        cls, timestamp_ms: int, data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        raw_streams = data.get("Streams", [])

        if isinstance(raw_streams, list):
            streams = raw_streams
        else:
            streams = list(raw_streams.values())

        return [
            {
                "timestamp": timestamp_ms,
                "type": s.get("Type"),
                "name": s.get("Name"),
                "language": s.get("Language"),
                "uri": s.get("Uri"),
                "path": s.get("Path"),
                "utc": s.get("Utc"),
            }
            for s in streams
            if isinstance(s, dict)
        ]


class ContentStreams(F1DataContainer):
    """Audio/video content stream references."""

    KEYFRAME_FILE: ClassVar[str | None] = "ContentStreams.json"
    STREAM_FILE: ClassVar[str | None] = "ContentStreams.jsonStream"

    keyframe: ContentStreamsKeyframe | None = None
    stream: ContentStreamsStream | None = None
