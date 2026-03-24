from datetime import timedelta
from typing import Any, ClassVar

import polars as pl
from pydantic import Field, field_validator, model_validator

from .base import F1DataContainer, F1Frame, F1Model, F1Stream
from .session import SessionType
from .timing_data import parse_lap_time

# TODO: Unify with timing_data and timing_app? I modeled it by file, but there is significant overlap


# TODO: In the case of a DNS (ie lando (RacingNumber 1) at 2026 Shanghai Race), a lot of these models collapse to {"value": ""}
# We could drop them (I think that decision should be made above this level)
class RankedValue(F1Model):
    position: int | None = Field(default=None)
    value: float | None = Field(default=None)

    @field_validator("value", mode="before")
    @classmethod
    def _parse_value(cls, v: str | float | int | None) -> float | None:
        if v == "" or v is None:
            return None
        return float(v)


class PersonalBestLapTime(F1Model):
    lap: int | None = Field(default=None)
    position: int | None = Field(default=None)
    time: timedelta | None = Field(alias="Value", default=None)

    @field_validator("time", mode="before")
    @classmethod
    def _parse_time(cls, v: timedelta | str | None) -> timedelta | None:
        if v == "" or v is None:
            return None
        if isinstance(v, str):
            return parse_lap_time(v)
        return v


class BestSpeeds(F1Model):
    i1: RankedValue = Field(alias="I1")
    i2: RankedValue = Field(alias="I2")
    fl: RankedValue = Field(alias="FL")
    st: RankedValue = Field(alias="ST")


class TimingStatsLine(F1Model):
    line: int
    racing_number: str
    personal_best_lap_time: PersonalBestLapTime
    best_sectors: list[RankedValue]
    best_speeds: BestSpeeds


class TimingStatsKeyframe(F1Frame):
    withheld: bool
    session_type: SessionType
    lines: list[TimingStatsLine]

    @model_validator(mode="before")
    @classmethod
    def _lines_to_list(cls, data: dict[str, object]) -> dict[str, object]:
        lines = data.get("Lines", {})
        if isinstance(lines, dict):
            data["Lines"] = list(lines.values())
        return data


class TimingStatsBestSpeedStream(F1Stream):
    SCHEMA: ClassVar[dict[str, pl.DataType]] = {
        "timestamp": pl.Duration("ms"),
        "car_number": pl.Utf8(),
        "trap": pl.Utf8(),
        "position": pl.UInt8(),
        "speed": pl.UInt16(),
    }

    @classmethod
    def _extract_rows(
        cls, timestamp_ms: int, data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for car_number, car_data in data.get("Lines", {}).items():
            for trap, trap_data in car_data.get("BestSpeeds", {}).items():
                if not isinstance(trap_data, dict):
                    continue
                value = trap_data.get("Value")
                rows.append(
                    {
                        "timestamp": timestamp_ms,
                        "car_number": car_number,
                        "trap": trap,
                        "position": trap_data.get("Position"),
                        "speed": int(value) if value else None,
                    }
                )
        return rows


class TimingStatsBestSectorStream(F1Stream):
    SCHEMA: ClassVar[dict[str, pl.DataType]] = {
        "timestamp": pl.Duration("ms"),
        "car_number": pl.Utf8(),
        "sector": pl.UInt8(),
        "position": pl.UInt8(),
        "time_seconds": pl.Float64(),
    }

    @classmethod
    def _extract_rows(
        cls, timestamp_ms: int, data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for car_number, car_data in data.get("Lines", {}).items():
            raw_sectors = car_data.get("BestSectors", {})
            if isinstance(raw_sectors, list):
                sector_items = ((str(i), s) for i, s in enumerate(raw_sectors))
            else:
                sector_items = raw_sectors.items()

            for sector_idx, sector_data in sector_items:
                if not isinstance(sector_data, dict):
                    continue
                value = sector_data.get("Value")
                rows.append(
                    {
                        "timestamp": timestamp_ms,
                        "car_number": car_number,
                        "sector": int(sector_idx),
                        "position": sector_data.get("Position"),
                        "time_seconds": float(value) if value else None,
                    }
                )
        return rows


class TimingStatsPersonalBestStream(F1Stream):
    SCHEMA: ClassVar[dict[str, pl.DataType]] = {
        "timestamp": pl.Duration("ms"),
        "car_number": pl.Utf8(),
        "position": pl.UInt8(),
        "lap_time": pl.Duration("ms"),
        "lap": pl.UInt16(),
    }

    @classmethod
    def _extract_rows(
        cls, timestamp_ms: int, data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for car_number, car_data in data.get("Lines", {}).items():
            pbl = car_data.get("PersonalBestLapTime")
            if isinstance(pbl, dict):
                rows.append(
                    {
                        "timestamp": timestamp_ms,
                        "car_number": car_number,
                        "position": pbl.get("Position"),
                        "lap_time": cls._parse_lap_time(pbl.get("Value")),
                        "lap": pbl.get("Lap"),
                    }
                )
        return rows


class TimingStats(F1DataContainer):
    """Session timing statistics — speeds, sectors, personal bests.

    Three streams from the same file, split by stat type.
    """

    KEYFRAME_FILE: ClassVar[str | None] = "TimingStats.json"
    STREAM_FILE: ClassVar[str | None] = "TimingStats.jsonStream"

    keyframe: TimingStatsKeyframe | None = None
    best_speeds: TimingStatsBestSpeedStream | None = None
    best_sectors: TimingStatsBestSectorStream | None = None
    personal_bests: TimingStatsPersonalBestStream | None = None

    @model_validator(mode="before")
    @classmethod
    def _split_stream(cls, raw: dict[str, object]) -> dict[str, object]:
        breakpoint()

        result: dict[str, object] = {}

        if "keyframe" in raw and raw["keyframe"] is not None:
            result["keyframe"] = raw["keyframe"]

        entries = raw.get("stream")
        if entries is not None:
            result["best_speeds"] = entries
            result["best_sectors"] = entries
            result["personal_bests"] = entries

        return result
