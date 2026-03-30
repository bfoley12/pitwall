from typing import ClassVar, cast, override

import polars as pl
from pydantic import Field, JsonValue, field_validator

from pitwall.api_handler.models.base import F1DataContainer, F1Frame, F1Model, F1Stream
from pitwall.api_handler.models.timing_data import LapTime
from pitwall.api_handler.registry import register


class DriverTrackerData(F1Model):
    position: int
    show_position: bool
    racing_number: int
    lap_time: LapTime | None = Field(default=None)
    lap_state: int  # TODO: Decode what these map to
    diff_to_ahead: float | None = Field(default=0.0)
    diff_to_leader: float | None = Field(default=0.0)
    overall_fastest: bool
    personal_fastest: bool

    @field_validator(
        "diff_to_ahead",
        "diff_to_leader",
        mode="before",
    )
    @classmethod
    def parse_gap(cls, v: object) -> float | int | None:
        if not isinstance(v, str) or v == "":
            return None
        cleaned = v.lstrip("+")
        if cleaned.endswith("L"):
            try:
                return -int(cleaned[:-1])
            except ValueError:
                return None
        try:
            return float(cleaned)
        except ValueError:
            return None


class DriverTrackerKeyframe(F1Frame):
    withheld: bool
    lines: list[DriverTrackerData]


class DriverTrackerStream(F1Stream):
    SCHEMA: ClassVar[dict[str, pl.DataType]] = {
        "timestamp": pl.Duration("ms"),
        "racing_number": pl.UInt8(),
        "lap_state": pl.Int16(),
        "lap_time": pl.Duration("ms"),
        "diff_to_ahead": pl.Float16(),
        "diff_to_leader": pl.Float16(),
    }

    @override
    @classmethod
    def _extract_rows(
        cls, timestamp_ms: int, data: dict[str, JsonValue]
    ) -> list[dict[str, JsonValue]]:
        rows: list[dict[str, JsonValue]] = []

        for racing_number, driver_data in cls._iter_lines(data):
            rows.append(
                {
                    "timestamp": timestamp_ms,
                    "racing_number": racing_number,
                    "lap_state": driver_data.get("LapState"),
                    "lap_time": cls._parse_lap_time(
                        cast(str, driver_data.get("LapTime"))
                    ),
                    "diff_to_ahead": cls.parse_gap(driver_data.get("DiffToAhead")),
                    "diff_to_leader": cls.parse_gap(driver_data.get("DiffToLeader")),
                }
            )

        return rows

    @staticmethod
    def parse_gap(v: object) -> float | int | None:
        if not isinstance(v, str) or v == "":
            return None
        cleaned = v.lstrip("+")
        if cleaned.endswith("L"):
            try:
                return -int(cleaned[:-1])
            except ValueError:
                return None
        try:
            return float(cleaned)
        except ValueError:
            return None


@register
class DriverTracker(F1DataContainer[DriverTrackerKeyframe, DriverTrackerStream]):
    KEYFRAME_FILE: ClassVar[str | None] = "DriverTracker.json"
    STREAM_FILE: ClassVar[str | None] = "DriverTracker.jsonStream"

    keyframe: DriverTrackerKeyframe
    stream: DriverTrackerStream
