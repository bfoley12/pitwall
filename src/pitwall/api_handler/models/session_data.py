from datetime import datetime
from typing import ClassVar, cast, override

import polars as pl
from pydantic import JsonValue

from pitwall.api_handler.models.base import (
    F1DataContainer,
    F1Frame,
    F1Model,
    F1Stream,
    ParsedValue,
)


class Lap(F1Model):
    utc: datetime
    lap: int


class Status(F1Model):
    utc: datetime
    track_status: str | None = None  # TODO: Make into StrEnum to control vocab
    session_status: str | None = None  # TODO: Make into StrEnum to control vocab


class SessionDataKeyframe(F1Frame):
    series: list[Lap]
    status_series: list[Status]


class SessionDataStream(F1Stream):
    SCHEMA: ClassVar[dict[str, pl.DataType]] = {
        "timestamp": pl.Duration("ms"),
        "series_type": pl.String(),
        "utc": pl.Datetime(),
        "lap": pl.UInt8(),
        "track_status": pl.String(),
        "session_status": pl.String(),
    }

    @override
    @classmethod
    def _extract_rows(
        cls, timestamp_ms: int, data: dict[str, JsonValue]
    ) -> list[dict[str, ParsedValue]]:
        rows: list[dict[str, ParsedValue]] = []
        row: dict[str, ParsedValue] = {}

        for series_type, entry in data.items():
            if isinstance(entry, list):
                entry = {"1": entry[0]} if entry else {}
            entry = cast(dict[str, JsonValue], entry)
            row = {
                "timestamp": timestamp_ms,
                "series_type": series_type,
                "utc": None,
                "lap": None,
                "track_status": None,
                "session_status": None,
            }
            if entry:
                for _, e in entry.items():
                    e = cast(dict[str, JsonValue], e)
                    utc_raw = e.get("Utc")
                    utc = cls._parse_utc(str(utc_raw)) if utc_raw is not None else None
                    row["utc"] = utc
                    if series_type == "Series":
                        row["lap"] = e.get("Lap")
                    elif series_type == "StatusSeries":
                        row["track_status"] = e.get("TrackStatus")
                        row["session_status"] = e.get("SessionStatus")

        rows.append(row)

        return rows


class SessionData(F1DataContainer[SessionDataKeyframe, SessionDataStream]):
    KEYFRAME_FILE: ClassVar[str | None] = "SessionData.json"
    STREAM_FILE: ClassVar[str | None] = "SessionData.jsonStream"

    keyframe: SessionDataKeyframe
    stream: SessionDataStream
