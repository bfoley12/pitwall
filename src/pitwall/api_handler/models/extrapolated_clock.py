from datetime import datetime, timedelta
from typing import ClassVar, override

import polars as pl
from pydantic import JsonValue, field_validator

from pitwall.api_handler.models.base import (
    F1DataContainer,
    F1Frame,
    F1Stream,
    ParsedValue,
)
from pitwall.api_handler.registry import register


class ExtrapolatedClockKeyframe(F1Frame):
    utc: datetime
    remaining: timedelta
    extrapolating: bool

    # @field_validator("utc", mode="before")
    # @classmethod
    # def format_utc(cls, v: str) -> datetime:
    #     breakpoint()
    #     return datetime.fromisoformat(v)


class ExtrapolatedClockStream(F1Stream):
    SCHEMA: ClassVar[dict[str, pl.DataType]] = {
        "timestamp": pl.Duration("ms"),
        "utc": pl.Datetime("ms"),
        "remaining": pl.Duration("ms"),
        "extrapolating": pl.Boolean(),
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
        remaining_raw = data.get("Remaining")
        remaining = (
            cls._parse_remaining(cls._as_str(remaining_raw))
            if isinstance(remaining_raw, str)
            else None
        )
        utc_raw = data.get("Utc")
        utc = cls._parse_utc(cls._as_str(utc_raw)) if isinstance(utc_raw, str) else None
        return [
            {
                "timestamp": timestamp_ms,
                "utc": utc,
                "remaining": remaining,
                "extrapolating": data.get("Extrapolating"),
            }
        ]


@register
class ExtrapolatedClock(
    F1DataContainer[ExtrapolatedClockKeyframe, ExtrapolatedClockStream]
):
    KEYFRAME_FILE: ClassVar[str | None] = "ExtrapolatedClock.json"
    STREAM_FILE: ClassVar[str | None] = "ExtrapolatedClock.jsonStream"

    keyframe: ExtrapolatedClockKeyframe
    stream: ExtrapolatedClockStream
