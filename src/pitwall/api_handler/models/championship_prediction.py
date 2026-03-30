from typing import ClassVar, override

import polars as pl
from pydantic import JsonValue

from pitwall.api_handler.registry import register

from .base import F1DataContainer, F1Frame, F1Model, F1Stream, ParsedValue

# ── Keyframe ──────────────────────────────────────────


class DriverPrediction(F1Model):
    racing_number: str
    current_position: int
    predicted_position: int
    current_points: float
    predicted_points: float


class TeamPrediction(F1Model):
    team_name: str
    current_position: int
    predicted_position: int
    current_points: float
    predicted_points: float


class ChampionshipPredictionKeyframe(F1Frame):
    """Pre-race standings and predicted end-of-race standings."""

    drivers: dict[str, DriverPrediction]
    teams: dict[str, TeamPrediction]


# ── Stream ────────────────────────────────────────────


class ChampionshipPredictionStream(F1Stream):
    """Unified driver + team prediction stream.

    Each row is either a driver or team update, distinguished by
    the ``entry_type`` column.  ``entry_key`` holds the car number
    for drivers or the team name for teams.
    """

    SCHEMA: ClassVar[dict[str, pl.DataType]] = {
        "timestamp": pl.Duration("ms"),
        "entry_type": pl.Categorical(),
        "entry_key": pl.Utf8(),
        "predicted_position": pl.UInt8(),
        "predicted_points": pl.Float64(),
    }

    @override
    @classmethod
    def _extract_rows(
        cls, timestamp_ms: int, data: dict[str, JsonValue]
    ) -> list[dict[str, ParsedValue]]:
        rows: list[dict[str, ParsedValue]] = []

        for key, update in cls._as_dict(data.get("Drivers")).items():
            if not isinstance(update, dict):
                continue
            rows.append(
                {
                    "timestamp": timestamp_ms,
                    "entry_type": "driver",
                    "entry_key": key,
                    "predicted_position": update.get("PredictedPosition"),
                    "predicted_points": update.get("PredictedPoints"),
                }
            )

        for key, update in cls._as_dict(data.get("Teams")).items():
            if not isinstance(update, dict):
                continue
            rows.append(
                {
                    "timestamp": timestamp_ms,
                    "entry_type": "team",
                    "entry_key": key,
                    "predicted_position": update.get("PredictedPosition"),
                    "predicted_points": update.get("PredictedPoints"),
                }
            )

        return rows


# ── Container ─────────────────────────────────────────


@register
class ChampionshipPrediction(
    F1DataContainer[ChampionshipPredictionKeyframe, ChampionshipPredictionStream]
):
    KEYFRAME_FILE: ClassVar[str | None] = "ChampionshipPrediction.json"
    STREAM_FILE: ClassVar[str | None] = "ChampionshipPrediction.jsonStream"

    keyframe: ChampionshipPredictionKeyframe
    stream: ChampionshipPredictionStream
