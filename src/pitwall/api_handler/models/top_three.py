from typing import ClassVar, cast, override

import polars as pl
from pydantic import JsonValue

from pitwall.api_handler.models.base import (
    F1DataContainer,
    F1Frame,
    ParsedValue,
)
from pitwall.api_handler.models.driver_tracker import (
    DriverTrackerData,
    DriverTrackerStream,
)
from pitwall.api_handler.registry import register


class TopThreeInfo(DriverTrackerData):
    tla: str
    broadcast_name: str
    full_name: str
    team: str
    team_colour: str


class TopThreeKeyframe(F1Frame):
    withheld: bool
    lines: list[TopThreeInfo]


class TopThreeStream(DriverTrackerStream):
    SCHEMA: ClassVar[dict[str, pl.DataType]] = {
        "timestamp": pl.Duration("ms"),
        "position": pl.UInt8(),
        "racing_number": pl.UInt8(),
        "tla": pl.Utf8(),
        "broadcast_name": pl.Utf8(),
        "full_name": pl.Utf8(),
        "team": pl.Utf8(),
        "team_colour": pl.Utf8(),
        "lap_state": pl.Int16(),
        "lap_time": pl.Duration("ms"),
        "diff_to_ahead": pl.Float16(),
        "diff_to_leader": pl.Float16(),
    }

    @override
    @classmethod
    def _extract_rows(
        cls, timestamp_ms: int, data: dict[str, JsonValue]
    ) -> list[dict[str, ParsedValue]]:
        rows: list[dict[str, ParsedValue]] = []
        for position, driver_data in cls._iter_lines(data):
            rows.append(
                {
                    "timestamp": timestamp_ms,
                    "position": int(position) + 1,
                    "racing_number": driver_data.get("RacingNumber"),
                    "tla": driver_data.get("Tla"),
                    "broadcast_name": driver_data.get("BroadcastName"),
                    "full_name": driver_data.get("FullName"),
                    "team": driver_data.get("Team"),
                    "team_colour": driver_data.get("TeamColour"),
                    "lap_state": driver_data.get("LapState"),
                    "lap_time": cls._parse_lap_time(
                        cast(str, driver_data.get("LapTime"))
                    ),
                    "diff_to_ahead": cls.parse_gap(driver_data.get("DiffToAhead")),
                    "diff_to_leader": cls.parse_gap(driver_data.get("DiffToLeader")),
                }
            )
        return rows


@register
class TopThree(F1DataContainer[TopThreeKeyframe, TopThreeStream]):
    KEYFRAME_FILE: ClassVar[str | None] = "TopThree.json"
    STREAM_FILE: ClassVar[str | None] = "TopThree.jsonStream"

    keyframe: TopThreeKeyframe
    stream: TopThreeStream
