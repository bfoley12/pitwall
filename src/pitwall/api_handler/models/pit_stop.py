from typing import ClassVar, override

import polars as pl
from pydantic import JsonValue

from pitwall.api_handler.models.base import F1DataContainer, F1Frame, F1Stream
from pitwall.api_handler.registry import register


# TODO: handle errors better when files aren't found (ie PitStop.json in 2023 Abu Dhabi Race)
class PitStopKeyframe(F1Frame):
    racing_number: int
    pit_stop_time: float
    pit_lane_time: float
    lap: int


class PitStopStream(F1Stream):
    SCHEMA: ClassVar[dict[str, pl.DataType]] = {
        "timestamp": pl.Duration("ms"),
        "racing_number": pl.UInt8(),
        "pit_stop_time": pl.Float16(),
        "pit_lane_time": pl.Float16(),
        "lap": pl.UInt8(),
    }

    @override
    @classmethod
    def _build_dataframe(cls, entries: list[dict[str, JsonValue]]) -> pl.DataFrame:
        rows: list[dict[str, JsonValue]] = []
        for entry in entries:
            ts_ms = cls._parse_timestamp(
                entry["Timestamp"] if isinstance(entry["Timestamp"], str) else "0"
            )
            row = cls._extract_rows(
                ts_ms, entry["Data"] if isinstance(entry["Data"], dict) else {}
            )
            if row[0]["lap"] is None:
                row[0]["lap"] = rows[-1]["lap"]
            rows.extend(row)
        return pl.DataFrame(rows, schema=cls.SCHEMA)


@register
class PitStop(F1DataContainer[PitStopKeyframe, PitStopStream]):
    KEYFRAME_FILE: ClassVar[str | None] = "PitStop.json"
    STREAM_FILE: ClassVar[str | None] = "PitStop.jsonStream"

    keyframe: PitStopKeyframe
    stream: PitStopStream
