from collections.abc import Iterable
from datetime import timedelta
from typing import ClassVar, override

import polars as pl
from pydantic import Field, JsonValue, field_validator, model_validator

from pitwall.api_handler.registry import register

from .base import F1DataContainer, F1Frame, F1Model, F1Stream, ParsedValue
from .session import SessionType
from .timing_data import parse_lap_time


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
    def _lines_to_list(cls, data: dict[str, JsonValue]) -> dict[str, JsonValue]:
        lines = data.get("Lines")
        if isinstance(lines, dict):
            data["Lines"] = [v for v in lines.values()]
        return data


class TimingStatsStream(F1Stream):
    """Unified stream for timing stats — best speeds, best sectors, personal bests.

    ``stat_type`` discriminates rows: "speed", "sector", or "personal_best".
    Columns specific to each type are null for other types.
    """

    SCHEMA: ClassVar[dict[str, pl.DataType]] = {
        "timestamp": pl.Duration("ms"),
        "car_number": pl.Utf8(),
        "stat_type": pl.Categorical(),
        "position": pl.UInt8(),
        # speed-specific
        "trap": pl.Utf8(),
        "speed": pl.UInt16(),
        # sector-specific
        "sector": pl.UInt8(),
        "time_seconds": pl.Float64(),
        # personal-best-specific
        "lap_time": pl.Duration("ms"),
        "lap": pl.UInt16(),
    }

    @override
    @classmethod
    def _extract_rows(
        cls, timestamp_ms: int, data: dict[str, JsonValue]
    ) -> list[dict[str, ParsedValue]]:
        rows: list[dict[str, ParsedValue]] = []

        for car_number, car_data in cls._iter_lines(data):
            # ── Best speeds ───────────────────────────
            best_speeds = cls._as_dict(car_data.get("BestSpeeds"))
            for trap, trap_data in best_speeds.items():
                if not isinstance(trap_data, dict):
                    continue
                value = trap_data.get("Value")
                rows.append(
                    {
                        "timestamp": timestamp_ms,
                        "car_number": car_number,
                        "stat_type": "speed",
                        "position": trap_data.get("Position"),
                        "trap": trap,
                        "speed": int(value) if isinstance(value, (str, int)) else None,
                        "sector": None,
                        "time_seconds": None,
                        "lap_time": None,
                        "lap": None,
                    }
                )

            # ── Best sectors ──────────────────────────
            raw_sectors = car_data.get("BestSectors")
            if isinstance(raw_sectors, list):
                sector_items: Iterable[tuple[str, JsonValue]] = (
                    (str(i), s) for i, s in enumerate(raw_sectors)
                )
            elif isinstance(raw_sectors, dict):
                sector_items = raw_sectors.items()
            else:
                sector_items = ()

            for sector_idx, sector_data in sector_items:
                if not isinstance(sector_data, dict):
                    continue
                value = sector_data.get("Value")
                rows.append(
                    {
                        "timestamp": timestamp_ms,
                        "car_number": car_number,
                        "stat_type": "sector",
                        "position": sector_data.get("Position"),
                        "trap": None,
                        "speed": None,
                        "sector": int(sector_idx),
                        "time_seconds": float(value)
                        if isinstance(value, (str, int, float))
                        else None,
                        "lap_time": None,
                        "lap": None,
                    }
                )

            # ── Personal best ─────────────────────────
            pbl = car_data.get("PersonalBestLapTime")
            if isinstance(pbl, dict):
                value = pbl.get("Value")
                rows.append(
                    {
                        "timestamp": timestamp_ms,
                        "car_number": car_number,
                        "stat_type": "personal_best",
                        "position": pbl.get("Position"),
                        "trap": None,
                        "speed": None,
                        "sector": None,
                        "time_seconds": None,
                        "lap_time": cls._parse_lap_time(value)
                        if isinstance(value, str)
                        else None,
                        "lap": pbl.get("Lap"),
                    }
                )

        return rows


@register
class TimingStats(F1DataContainer[TimingStatsKeyframe, TimingStatsStream]):
    """Session timing statistics — speeds, sectors, personal bests.

    Three streams from the same file, split by stat type.
    """

    KEYFRAME_FILE: ClassVar[str | None] = "TimingStats.json"
    STREAM_FILE: ClassVar[str | None] = "TimingStats.jsonStream"

    keyframe: TimingStatsKeyframe
    stream: TimingStatsStream

    @model_validator(mode="before")
    @classmethod
    def _split_stream(cls, raw: dict[str, object]) -> dict[str, object]:
        result: dict[str, object] = {}

        if "keyframe" in raw and raw["keyframe"] is not None:
            result["keyframe"] = raw["keyframe"]

        entries = raw.get("stream")
        if entries is not None:
            result["best_speeds"] = entries
            result["best_sectors"] = entries
            result["personal_bests"] = entries

        return result
