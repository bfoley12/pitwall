from datetime import datetime
from typing import ClassVar, override

import polars as pl
from pydantic import Field, JsonValue

from pitwall.api_handler.models.base import (
    F1DataContainer,
    F1Frame,
    F1Model,
    F1Stream,
    ParsedValue,
)
from pitwall.api_handler.models.pit_stop import PitStopKeyframe
from pitwall.api_handler.registry import register


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
        cls, timestamp_ms: int, data: dict[str, JsonValue]
    ) -> list[dict[str, ParsedValue]]:
        rows: list[dict[str, ParsedValue]] = []
        pit_times = cls._as_dict(data.get("PitTimes"))

        for racing_number, line in pit_times.items():
            if isinstance(line, dict):
                value = next(iter(line.values()), None)
            elif isinstance(line, list) and line:
                value = line[0]
            else:
                continue

            if not isinstance(value, dict):
                continue

            pit_stop = cls._as_dict(value.get("PitStop"))

            rows.append(
                {
                    "timestamp": timestamp_ms,
                    "utc": value["Timestamp"]
                    if isinstance(value.get("Timestamp"), str)
                    else None,
                    "racing_number": racing_number,
                    "pit_stop_time": pit_stop.get("PitStopTime"),
                    "pit_lane_time": pit_stop.get("PitLaneTime"),
                    "lap": pit_stop.get("Lap"),
                }
            )

        return rows


@register
class PitStopSeries(F1DataContainer[PitStopSeriesKeyframe, PitStopSeriesStream]):
    KEYFRAME_FILE: ClassVar[str | None] = "PitStopSeries.json"
    STREAM_FILE: ClassVar[str | None] = "PitStopSeries.jsonStream"

    keyframe: PitStopSeriesKeyframe
    stream: PitStopSeriesStream
