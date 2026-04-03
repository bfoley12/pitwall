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


class TlaRcmKeyframe(F1Frame):
    timestamp: datetime
    message: str


class TlaRcmStream(F1Stream):
    SCHEMA: ClassVar[dict[str, pl.DataType]] = {
        "timestamp": pl.Duration("ms"),
        "utc": pl.Datetime(),
        "message": pl.String(),
    }

    @classmethod
    def _parse_remaining(cls, s: str) -> int:
        h, m, sec = s.split(":")
        return int(h) * 3_600_000 + int(m) * 60_000 + int(sec) * 1_000

    # TODO: Unify this and _parse_remaining into F1Stream helper "handle_utc" or something
    # (we have _parse_utc but not sure it plays well with polars)
    @override
    @classmethod
    def _extract_rows(
        cls, timestamp_ms: int, data: dict[str, JsonValue]
    ) -> list[dict[str, ParsedValue]]:
        utc_raw = data.get("Timestamp")
        utc = cls._parse_utc(cls._as_str(utc_raw)) if isinstance(utc_raw, str) else None
        return [{"timestamp": timestamp_ms, "utc": utc, "message": data.get("Message")}]


@register
class TlaRcm(F1DataContainer[TlaRcmKeyframe, TlaRcmStream]):
    KEYFRAME_FILE: ClassVar[str | None] = "TlaRcm.json"
    STREAM_FILE: ClassVar[str | None] = "TlaRcm.jsonStream"

    keyframe: TlaRcmKeyframe
    stream: TlaRcmStream
