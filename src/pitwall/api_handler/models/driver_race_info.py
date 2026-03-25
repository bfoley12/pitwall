import re
from enum import IntEnum
from typing import ClassVar, override

import polars as pl
from pydantic import JsonValue, model_validator

from pitwall.api_handler.registry import register

from .base import F1DataContainer, F1Frame, F1Model, F1Stream


class Catching(IntEnum):
    NOT_CLOSING = 1
    CLOSING = 2


class OvertakeState(IntEnum):
    LOST_POSITION = 0
    NEUTRAL = 1
    GAINED_POSITION = 2


_LEADER_LAP_RE = re.compile(r"^LAP\s+(\d+)$")
_LAPPED_RE = re.compile(r"^\+(\d+)\s+LAP$")
_TIME_GAP_RE = re.compile(r"^\+(?:(\d+):)?(\d+\.\d+)$")

_CATCHING_MAP: dict[int, str] = {v.value: v.name.lower() for v in Catching}
_OVERTAKE_MAP: dict[int, str] = {v.value: v.name.lower() for v in OvertakeState}


def _parse_gap(raw: str | None) -> tuple[float | None, int, bool, int | None]:
    if raw is None or raw == "":
        return (None, 0, False, None)

    m = _LEADER_LAP_RE.match(raw)
    if m:
        return (None, 0, True, int(m.group(1)))

    m = _LAPPED_RE.match(raw)
    if m:
        return (None, int(m.group(1)), False, None)

    m = _TIME_GAP_RE.match(raw)
    if m:
        minutes = int(m.group(1)) if m.group(1) else 0
        seconds = float(m.group(2))
        return (minutes * 60.0 + seconds, 0, False, None)

    return (None, 0, False, None)


def _parse_interval(raw: str | None) -> float | None:
    if raw is None or raw == "":
        return None

    if _LEADER_LAP_RE.match(raw):
        return None

    m = _TIME_GAP_RE.match(raw)
    if m:
        minutes = int(m.group(1)) if m.group(1) else 0
        seconds = float(m.group(2))
        return minutes * 60.0 + seconds

    return None


# ── Keyframe ──────────────────────────────────────────


class DriverRaceInfoLine(F1Model):
    racing_number: str
    position: str
    gap: str = ""
    interval: str = ""
    pit_stops: int | None = None
    catching: int | None = None
    overtake_state: int | None = None
    is_out: bool = False


class DriverRaceInfoKeyframe(F1Frame):
    """Final race standings from DriverRaceInfo.json."""

    drivers: dict[str, DriverRaceInfoLine]

    def __getitem__(self, car_number: str) -> DriverRaceInfoLine:
        return self.drivers[car_number]

    @model_validator(mode="before")
    @classmethod
    def _wrap(cls, data: dict[str, JsonValue]) -> dict[str, JsonValue]:
        if "drivers" not in data:
            return {"drivers": data}
        return data


# ── Stream ────────────────────────────────────────────


_GRID_SCHEMA: dict[str, pl.DataType] = {
    "position": pl.UInt8(),
    "car_number": pl.Utf8(),
}


class DriverRaceInfoStream(F1Stream):
    """Race event stream with starting grid extraction.

    The first jsonStream entry is a grid snapshot. All subsequent
    entries are sparse delta updates.

    Attributes:
        frame: Sparse delta updates throughout the session.
            Null fields mean "no change from previous state."
        starting_grid: Grid order extracted from the first entry.
    """

    SCHEMA: ClassVar[dict[str, pl.DataType]] = {
        "timestamp": pl.Duration("ms"),
        "car_number": pl.Utf8(),
        "position": pl.UInt8(),
        "gap_seconds": pl.Float64(),
        "laps_behind": pl.UInt8(),
        "interval_seconds": pl.Float64(),
        "catching": pl.Utf8(),
        "overtake_state": pl.Utf8(),
        "is_leader": pl.Boolean(),
        "leader_lap": pl.UInt16(),
        "pit_stops": pl.UInt8(),
        "is_out": pl.Boolean(),
    }

    @override
    @classmethod
    def _extract_rows(
        cls, timestamp_ms: int, data: dict[str, JsonValue]
    ) -> list[dict[str, JsonValue]]:
        rows: list[dict[str, JsonValue]] = []

        for car_number, update in data.items():
            if not isinstance(update, dict):
                continue

            gap_raw = update.get("Gap")
            interval_raw = update.get("Interval")
            gap_seconds, laps_behind, is_leader, leader_lap = _parse_gap(
                gap_raw if isinstance(gap_raw, str) else None
            )
            interval_seconds = _parse_interval(
                interval_raw if isinstance(interval_raw, str) else None
            )

            catching_raw = update.get("Catching")
            overtake_raw = update.get("OvertakeState")
            position_raw = update.get("Position")

            rows.append(
                {
                    "timestamp": timestamp_ms,
                    "car_number": car_number,
                    "position": int(position_raw)
                    if isinstance(position_raw, (str, int))
                    else None,
                    "gap_seconds": gap_seconds,
                    "laps_behind": laps_behind,
                    "interval_seconds": interval_seconds,
                    "catching": _CATCHING_MAP[catching_raw]
                    if isinstance(catching_raw, int)
                    else None,
                    "overtake_state": _OVERTAKE_MAP[overtake_raw]
                    if isinstance(overtake_raw, int)
                    else None,
                    "is_leader": is_leader,
                    "leader_lap": leader_lap,
                    "pit_stops": update.get("PitStops"),
                    "is_out": update.get("IsOut"),
                }
            )

        return rows

    @classmethod
    def _build_starting_grid(cls, first_entry: dict[str, JsonValue]) -> pl.DataFrame:
        rows: list[dict[str, str | int]] = []
        for car_number, data in first_entry.items():
            if not isinstance(data, dict):
                continue
            position = data.get("Position")
            if isinstance(position, (str, int)):
                rows.append(
                    {
                        "car_number": car_number,
                        "position": int(position),
                    }
                )
        return pl.DataFrame(rows, schema=_GRID_SCHEMA).sort("position")

    @model_validator(mode="before")
    @classmethod
    def _from_entries(
        cls, raw: list[dict[str, JsonValue]] | dict[str, object]
    ) -> dict[str, object]:
        if not isinstance(raw, list) or not raw:
            return raw if isinstance(raw, dict) else {}

        first_data = raw[0].get("Data")
        if not isinstance(first_data, dict):
            return {}

        starting_grid = cls._build_starting_grid(first_data)
        frame = cls._build_dataframe(raw[1:])

        frame = (
            frame.with_row_index("_row_idx")
            .filter(
                pl.col("_row_idx")
                == pl.col("_row_idx").last().over("timestamp", "car_number")
            )
            .drop("_row_idx")
        )
        frame = frame.with_columns(
            pl.col("catching").cast(pl.Categorical),
            pl.col("overtake_state").cast(pl.Categorical),
        )

        return {
            "frame": frame,
            "data": starting_grid,
        }


# ── Container ─────────────────────────────────────────


@register
class DriverRaceInfo(F1DataContainer[DriverRaceInfoKeyframe, DriverRaceInfoStream]):
    """Driver race info — keyframe + event stream.

    Key column behaviors on stream.frame:
    - leader_lap: Only populated for the current race leader
      when they cross the start/finish line. LAP 0 = formation lap.
    - pit_stops: Cumulative count, emitted once per stop.
    - is_out: True when a car permanently exits (DNS/DNF/retirement).
      Always the final event for that car.
    """

    KEYFRAME_FILE: ClassVar[str | None] = "DriverRaceInfo.json"
    STREAM_FILE: ClassVar[str | None] = "DriverRaceInfo.jsonStream"

    keyframe: DriverRaceInfoKeyframe
    stream: DriverRaceInfoStream
