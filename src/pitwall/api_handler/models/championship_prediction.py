from __future__ import annotations

from typing import Any, ClassVar

import polars as pl
from pydantic import ConfigDict

from .base import F1Model

# ── Keyframe (ChampionshipPrediction.json) ────────────────────────


class DriverPrediction(F1Model):
    """Championship prediction for a single driver."""

    racing_number: str
    current_position: int
    predicted_position: int
    current_points: float
    predicted_points: float


class TeamPrediction(F1Model):
    """Championship prediction for a single team."""

    team_name: str
    current_position: int
    predicted_position: int
    current_points: float
    predicted_points: float


class ChampionshipPrediction(F1Model):
    """Final-state championship predictions from ChampionshipPrediction.json.

    Contains pre-race standings (current) and F1's predicted end-of-race
    standings for both drivers and constructors.
    """

    drivers: dict[str, DriverPrediction]
    teams: dict[str, TeamPrediction]

    def driver(self, racing_number: str) -> DriverPrediction:
        return self.drivers[racing_number]

    def team(self, team_key: str) -> TeamPrediction:
        return self.teams[team_key]


# ── Stream (ChampionshipPrediction.jsonStream) ────────────────────

_DRIVER_STREAM_SCHEMA: dict[str, pl.DataType] = {
    "timestamp": pl.Duration("ms"),
    "racing_number": pl.Utf8(),
    "predicted_position": pl.UInt8(),
    "predicted_points": pl.Float64(),
}

_TEAM_STREAM_SCHEMA: dict[str, pl.DataType] = {
    "timestamp": pl.Duration("ms"),
    "team_key": pl.Utf8(),
    "predicted_position": pl.UInt8(),
    "predicted_points": pl.Float64(),
}

_DriverStreamRow = dict[str, int | float | str | None]
_TeamStreamRow = dict[str, int | float | str | None]


def parse_timestamp(ts: str) -> int:
    """Parse 'HH:MM:SS.fff' to milliseconds."""
    h, m, rest = ts.split(":")
    s, ms = rest.split(".")
    return int(h) * 3_600_000 + int(m) * 60_000 + int(s) * 1_000 + int(ms)


class ChampionshipPredictionStream(F1Model):
    """Championship prediction deltas over time.

    Sparse updates showing how F1's predicted championship standings
    shifted throughout the session. Correlate with race events
    (safety cars, pit stops, overtakes) for analysis.

    Attributes:
        drivers: Delta updates for driver predictions.
            Columns: timestamp, racing_number, predicted_position, predicted_points.
            Null fields mean "no change from previous value."
        teams: Delta updates for team predictions.
            Columns: timestamp, team_key, predicted_position, predicted_points.
            Null fields mean "no change from previous value."
    """

    model_config: ClassVar[ConfigDict] = ConfigDict(arbitrary_types_allowed=True)

    drivers: pl.DataFrame
    teams: pl.DataFrame


def build_championship_prediction_stream(
    entries: list[dict[str, Any]],
) -> ChampionshipPredictionStream:
    """Parse decoded ChampionshipPrediction.jsonStream entries.

    Parameters
    ----------
    entries
        Output of ``_decode_response()`` for ChampionshipPrediction.jsonStream.
        Each entry has ``"Timestamp"`` (str) and ``"Data"`` (dict).
    """
    driver_rows: list[_DriverStreamRow] = []
    team_rows: list[_TeamStreamRow] = []

    for entry in entries:
        ts_ms: int = parse_timestamp(entry["Timestamp"])
        data: dict[str, Any] = entry["Data"]

        drivers: dict[str, dict[str, Any]] = data.get("Drivers", {})
        for racing_number, update in drivers.items():
            driver_rows.append(
                {
                    "timestamp": ts_ms,
                    "racing_number": racing_number,
                    "predicted_position": update.get("PredictedPosition"),
                    "predicted_points": update.get("PredictedPoints"),
                }
            )

        teams: dict[str, dict[str, Any]] = data.get("Teams", {})
        for team_key, update in teams.items():
            team_rows.append(
                {
                    "timestamp": ts_ms,
                    "team_key": team_key,
                    "predicted_position": update.get("PredictedPosition"),
                    "predicted_points": update.get("PredictedPoints"),
                }
            )

    return ChampionshipPredictionStream(
        drivers=pl.DataFrame(driver_rows, schema=_DRIVER_STREAM_SCHEMA),
        teams=pl.DataFrame(team_rows, schema=_TEAM_STREAM_SCHEMA),
    )
