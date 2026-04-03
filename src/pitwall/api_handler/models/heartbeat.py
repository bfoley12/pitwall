from datetime import datetime
from typing import ClassVar, override

import polars as pl
from pydantic import JsonValue

from pitwall.api_handler.models.base import (
    F1DataContainer,
    F1Frame,
    F1Stream,
    ParsedValue,
)
from pitwall.api_handler.registry import register


class HeartbeatKeyframe(F1Frame):
    utc: datetime


class HeartbeatStream(F1Stream):
    SCHEMA: ClassVar[dict[str, pl.DataType]] = {
        "timestamp": pl.Duration("ms"),
        "utc": pl.Datetime("ns"),
    }

    @classmethod
    def _parse_remaining(cls, s: str) -> int:
        h, m, sec = s.split(":")
        return int(h) * 3_600_000 + int(m) * 60_000 + int(sec) * 1_000

    @override
    @classmethod
    def _extract_rows(
        cls, timestamp_ms: int, data: dict[str, JsonValue]
    ) -> list[dict[str, ParsedValue]]:
        utc_raw = data.get("Utc")
        utc = cls._parse_utc(cls._as_str(utc_raw)) if isinstance(utc_raw, str) else None
        return [
            {
                "timestamp": timestamp_ms,
                "utc": utc,
            }
        ]


@register
class Heartbeat(F1DataContainer[HeartbeatKeyframe, HeartbeatStream]):
    KEYFRAME_FILE: ClassVar[str | None] = "Heartbeat.json"
    STREAM_FILE: ClassVar[str | None] = "Heartbeat.jsonStream"

    keyframe: HeartbeatKeyframe
    stream: HeartbeatStream
