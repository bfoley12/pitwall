from __future__ import annotations

import re
from enum import IntEnum
from typing import Any, ClassVar

import polars as pl
from pydantic import ConfigDict

from .base import F1Model


class Catching(IntEnum):
    """Whether a driver is closing on the car ahead."""

    NOT_CLOSING = 1
    CLOSING = 2


class OvertakeState(IntEnum):
    """Transient position-change indicator (~5s duration)."""

    LOST_POSITION = 0
    NEUTRAL = 1
    GAINED_POSITION = 2


_LEADER_LAP_RE = re.compile(r"^LAP\s+(\d+)$")
_LAPPED_RE = re.compile(r"^\+(\d+)\s+LAP$")
_TIME_GAP_RE = re.compile(r"^\+(?:(\d+):)?(\d+\.\d+)$")

_CATCHING_MAP: dict[int, str] = {v.value: v.name.lower() for v in Catching}
_OVERTAKE_MAP: dict[int, str] = {v.value: v.name.lower() for v in OvertakeState}


def _parse_gap(raw: str | None) -> tuple[float | None, int, bool, int | None]:
    """Parse a Gap string.

    Returns:
        (gap_seconds, laps_behind, is_leader, leader_lap)
    """
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
    """Parse an Interval string to seconds, or None."""
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


def _parse_timestamp(ts: str) -> int:
    """Parse 'HH:MM:SS.fff' to milliseconds."""
    h, m, rest = ts.split(":")
    s, ms = rest.split(".")
    return int(h) * 3_600_000 + int(m) * 60_000 + int(s) * 1_000 + int(ms)


_GRID_SCHEMA: dict[str, pl.DataType] = {
    "position": pl.UInt8(),
    "racing_number": pl.Utf8(),
}


def _build_starting_grid(
    first_entry: dict[str, Any],
) -> pl.DataFrame:
    """Extract the starting grid from the initial keyframe entry.

    The first line of the jsonStream contains every driver's grid
    position, racing number, and whether they started the race.
    """
    rows: list[dict[str, str | int | bool]] = []

    for racing_number, data in first_entry["Data"].items():
        rows.append(
            {
                "racing_number": racing_number,
                "position": int(data["Position"]),
                "is_out": data.get("IsOut", False),
            }
        )

    return pl.DataFrame(rows, schema=_GRID_SCHEMA).sort("position")


_STREAM_SCHEMA: dict[str, pl.DataType] = {
    "timestamp": pl.Duration("ms"),
    "racing_number": pl.Utf8(),
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

_StreamRow = dict[str, int | float | str | bool | None]


def _build_event_stream(
    entries: list[dict[str, Any]],
) -> pl.DataFrame:
    """Build the event stream DataFrame from all entries after the keyframe."""
    rows: list[_StreamRow] = []

    for entry in entries:
        ts_ms: int = _parse_timestamp(entry["Timestamp"])
        data: dict[str, dict[str, Any]] = entry["Data"]

        for racing_number, update in data.items():
            gap_raw: str | None = update.get("Gap")
            interval_raw: str | None = update.get("Interval")

            gap_seconds, laps_behind, is_leader, leader_lap = _parse_gap(gap_raw)
            interval_seconds: float | None = _parse_interval(interval_raw)

            catching_raw: int | None = update.get("Catching")
            overtake_raw: int | None = update.get("OvertakeState")

            row: _StreamRow = {
                "timestamp": ts_ms,
                "racing_number": racing_number,
                "position": (int(update["Position"]) if "Position" in update else None),
                "gap_seconds": gap_seconds,
                "laps_behind": laps_behind,
                "interval_seconds": interval_seconds,
                "catching": (
                    _CATCHING_MAP[catching_raw] if catching_raw is not None else None
                ),
                "overtake_state": (
                    _OVERTAKE_MAP[overtake_raw] if overtake_raw is not None else None
                ),
                "is_leader": is_leader,
                "leader_lap": leader_lap,
                "pit_stops": update.get("PitStops"),
                "is_out": update.get("IsOut"),
            }
            rows.append(row)

    df = pl.DataFrame(rows, schema=_STREAM_SCHEMA)

    # Deduplicate corrections: when multiple updates arrive for the
    # same car at the same timestamp, keep the last one (the API
    # occasionally re-sends or corrects values at identical timestamps).
    df = (
        df.with_row_index("_row_idx")
        .filter(
            pl.col("_row_idx")
            == pl.col("_row_idx").last().over("timestamp", "racing_number")
        )
        .drop("_row_idx")
    )

    return df.with_columns(
        pl.col("catching").cast(pl.Categorical),
        pl.col("overtake_state").cast(pl.Categorical),
    )


class DriverRaceInfo(F1Model):
    """Parsed DriverRaceInfo jsonStream data.

    Attributes:
        starting_grid: Grid positions from the initial keyframe.
            Columns: racing_number (str), position (UInt8).
            Reflects grid order before formation lap. Does NOT
            indicate DNS — all drivers appear regardless.
        events: Sparse delta updates throughout the session.
            Each row is a partial update for one car at one timestamp.
            Null fields mean "no change from previous state."

            Key column behaviors:
            - leader_lap: Only populated for the current race leader
              when they cross the start/finish line. Acts as the
              global race lap counter. LAP 0 = formation lap.
            - pit_stops: Cumulative pit stop count. Only emitted at
              the moment the count increments (once per stop).
            - is_out: True when a car permanently exits the session
              (DNS, DNF, retirement). Always the final event for
              that car. Multiple cars may go out simultaneously
              (e.g. first-lap incidents).
    """

    model_config: ClassVar[ConfigDict] = ConfigDict(arbitrary_types_allowed=True)

    starting_grid: pl.DataFrame
    # TODO: is_out sometimes updates very late for cars that DNS (cars 1, 5, 23, 81 in 2026 Shanghai Race have is_out = True ~113s into the race)
    events: pl.DataFrame


def build_driver_race_info(
    entries: list[dict[str, Any]],
) -> DriverRaceInfo:
    """Parse decoded DriverRaceInfo.jsonStream entries.

    Parameters
    ----------
    entries
        Output of ``_decode_response()`` for DriverRaceInfo.jsonStream.
        Each entry has ``"Timestamp"`` (str) and ``"Data"`` (dict).

    Returns
    -------
    DriverRaceInfo
        Contains ``starting_grid`` (from the initial keyframe) and
        ``events`` (all subsequent delta updates).
    """
    if not entries:
        return DriverRaceInfo(
            starting_grid=pl.DataFrame(schema=_GRID_SCHEMA),
            events=pl.DataFrame(schema=_STREAM_SCHEMA),
        )

    starting_grid = _build_starting_grid(entries[0])
    events = _build_event_stream(entries[1:])

    return DriverRaceInfo(
        starting_grid=starting_grid,
        events=events,
    )
