import re
from collections.abc import Iterable
from datetime import timedelta
from typing import Annotated, ClassVar, override

import polars as pl
from pydantic import BeforeValidator, Field, JsonValue
from pydantic.functional_validators import field_validator

from pitwall.api_handler.models.base import F1DataContainer, F1Frame, F1Model, F1Stream
from pitwall.api_handler.registry import register

# TODO: Decode what segment status is: observed values: 0, 2048, 2049, 2050, 2051, 2052, 2064


def parse_lap_time(value: str) -> timedelta:
    """Parse F1 lap time string like '1:26.933' or '26.933' into timedelta."""
    match = re.fullmatch(r"(?:(\d+):)?(\d+)\.(\d+)", value)
    if not match:
        raise ValueError(f"Invalid lap time: {value!r}")
    minutes = int(match.group(1) or 0)
    seconds = int(match.group(2))
    millis = int(match.group(3).ljust(3, "0")[:3])
    return timedelta(minutes=minutes, seconds=seconds, milliseconds=millis)


LapTime = Annotated[timedelta, BeforeValidator(parse_lap_time)]


class LastLapTime(F1Model):
    value: LapTime
    # TODO: Figure out what status maps to
    status: int
    overall_fastest: bool
    personal_fastest: bool


class BestLapTime(F1Model):
    value: LapTime
    lap: int


# Added optionals to allow reuse in timing_stats
class SpeedTrap(F1Model):
    value: str
    status: int
    overall_fastest: bool | None
    personal_fastest: bool | None


class Speeds(F1Model):
    # TODO: Figure out what i1, i2, fl, and st are
    i1: SpeedTrap = Field(alias="I1")
    i2: SpeedTrap = Field(alias="I2")
    fl: SpeedTrap = Field(alias="FL")
    st: SpeedTrap = Field(alias="ST")


class Segment(F1Model):
    # TODO: Figure out how status encodes
    status: int = Field(alias="Status")


class Sector(F1Model):
    stopped: bool
    previous_value: float
    segments: list[Segment]
    value: float
    # TODO: Figure out how status encodes
    status: int
    overall_fastest: bool
    personal_fastest: bool


class IntervalData(F1Model):
    value: str
    catching: bool


class TimingLine(F1Model):
    racing_number: str
    position: str
    line: int
    show_position: bool

    # Practice fields
    time_diff_to_fastest: float | int | None = None
    time_diff_to_position_ahead: float | int | None = Field(default=None)

    # Race fields
    gap_to_leader: float | int | None = Field(default=None)
    interval_to_position_ahead: IntervalData | None = Field(default=None)

    # Common
    retired: bool
    in_pit: bool
    pit_out: bool
    stopped: bool
    status: int
    number_of_laps: int
    number_of_pit_stops: int
    sectors: list[Sector]
    speeds: Speeds
    best_lap_time: BestLapTime
    last_lap_time: LastLapTime

    @field_validator(
        "time_diff_to_fastest",
        "time_diff_to_position_ahead",
        "gap_to_leader",
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


class TimingDataF1Keyframe(F1Frame):
    lines: dict[str, TimingLine]
    withheld: bool


class TimingDataF1Stream(F1Stream):
    """Segment status updates from TimingDataF1.jsonStream.

    Each row is a single segment status change for one car.
    Status values are bitmask flags (0, 2048, 2052, 2064, etc.)
    encoding segment completion state and personal/overall fastest.
    """

    SCHEMA: ClassVar[dict[str, pl.DataType]] = {
        "timestamp": pl.Duration("ms"),
        "car_number": pl.Utf8(),
        "sector": pl.UInt8(),
        "segment": pl.UInt8(),
        "status": pl.UInt16(),
    }

    @override
    @classmethod
    def _extract_rows(
        cls, timestamp_ms: int, data: dict[str, JsonValue]
    ) -> list[dict[str, JsonValue]]:
        rows: list[dict[str, JsonValue]] = []
        lines = cls._as_dict(data.get("Lines"))

        for car_number, car_data in lines.items():
            if not isinstance(car_data, dict):
                continue

            sectors = car_data.get("Sectors")
            if isinstance(sectors, list):
                sector_items: Iterable[tuple[int, JsonValue]] = enumerate(sectors)
            elif isinstance(sectors, dict):
                sector_items = ((int(k), v) for k, v in sectors.items())
            else:
                continue

            for sector_idx, sector_data in sector_items:
                if not isinstance(sector_data, dict):
                    continue

                segments = sector_data.get("Segments")
                if isinstance(segments, list):
                    segment_items: Iterable[tuple[int, JsonValue]] = enumerate(segments)
                elif isinstance(segments, dict):
                    segment_items = ((int(k), v) for k, v in segments.items())
                else:
                    continue

                for segment_idx, segment_data in segment_items:
                    if not isinstance(segment_data, dict):
                        continue
                    status = segment_data.get("Status")
                    if status is None:
                        continue
                    rows.append(
                        {
                            "timestamp": timestamp_ms,
                            "car_number": car_number,
                            "sector": sector_idx + 1,
                            "segment": segment_idx,
                            "status": status,
                        }
                    )

        return rows


@register
class TimingDataF1(F1DataContainer[TimingDataF1Keyframe, TimingDataF1Stream]):
    KEYFRAME_FILE: ClassVar[str | None] = "TimingDataF1.json"
    STREAM_FILE: ClassVar[str | None] = "TimingDataF1.jsonStream"

    keyframe: TimingDataF1Keyframe
    stream: TimingDataF1Stream
