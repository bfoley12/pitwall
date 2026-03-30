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


# TODO: Implement
class PositionKeyframe(F1Frame):
    pass


class PositionStream(F1Stream):
    SCHEMA: ClassVar[dict[str, pl.DataType]] = {
        "timestamp": pl.Duration("ms"),
        "utc": pl.Datetime(),
        "racing_number": pl.UInt8(),
        "status": pl.String(),
        # TODO: Determine best representation. Float64 is surely too large
        "x": pl.Float64(),
        "y": pl.Float64(),
        "z": pl.Float64(),
    }

    @override
    @classmethod
    def _extract_rows(
        cls, timestamp_ms: int, data: dict[str, JsonValue]
    ) -> list[dict[str, ParsedValue]]:
        rows: list[dict[str, ParsedValue]] = []
        utc_raw = data.get("Timestamp")
        utc = utc_raw if isinstance(utc_raw, str) else None
        entries = cls._as_dict(data.get("Entries"))

        for racing_number, entry in entries.items():
            if not isinstance(entry, dict):
                continue
            rows.append(
                {
                    "utc": utc,
                    "timestamp": timestamp_ms,
                    "racing_number": racing_number,
                    "status": entry.get("Status"),
                    "x": entry.get("X"),
                    "y": entry.get("Y"),
                    "z": entry.get("Z"),
                }
            )

        return rows


@register
class Position(F1DataContainer[PositionKeyframe, PositionStream]):
    STREAM_FILE: ClassVar[str | None] = "Position.z.jsonStream"

    stream: PositionStream
