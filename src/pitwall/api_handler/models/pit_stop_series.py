from datetime import datetime
from typing import Any, ClassVar, override

import polars as pl
from pydantic import Field

from pitwall.api_handler.models.base import F1DataContainer, F1Frame, F1Model, F1Stream
from pitwall.api_handler.models.pit_stop import PitStopKeyframe


class PitStopInfo(F1Model):
    utc: datetime = Field(alias="Timestamp")
    pit_stop: PitStopKeyframe


class PitStopSeriesKeyframe(F1Frame):
    pit_times: dict[int, list[PitStopInfo]]


class PitStopSeriesStream(F1Stream):
    SCHEMA: ClassVar[dict[str, pl.DataType]] = {
        "timestamp": pl.Duration("ms"),
        "utc": pl.Datetime(),
        "racing_number": pl.UInt8(),
        "pit_stop_time": pl.Float16(),
        "pit_lane_time": pl.Float16(),
        "lap": pl.UInt8(),
    }

    @override
    @classmethod
    def _extract_rows(
        cls, timestamp_ms: int, data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        value: dict[str, Any] = {}
        for racing_number, line in data["PitTimes"].items():
            value = next(iter(line.values())) if isinstance(line, dict) else line[0]
            rows.extend(
                [
                    {
                        "timestamp": timestamp_ms,
                        "utc": cls._parse_utc(value["Timestamp"])
                        if "Timestamp" in value
                        else None,
                        "racing_number": racing_number,
                        "pit_stop_time": value["PitStop"].get("PitStopTime"),
                        "pit_lane_time": value["PitStop"].get("PitLaneTime"),
                        "lap": value["PitStop"].get("Lap"),
                    }
                ]
            )
        return rows


class PitStopSeries(F1DataContainer):
    KEYFRAME_FILE: ClassVar[str | None] = "PitStopSeries.json"
    STREAM_FILE: ClassVar[str | None] = "PitStopSeries.jsonStream"

    keyframe: PitStopSeriesKeyframe
    stream: PitStopSeriesStream
