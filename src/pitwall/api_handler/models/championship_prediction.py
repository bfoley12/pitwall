from typing import Any, ClassVar

import polars as pl
from pydantic import model_validator

from .base import F1DataContainer, F1Model, F1Stream


# TODO: TEMP for compatibility while I update
def parse_timestamp(ts: str) -> int:
    h, m, rest = ts.split(":")
    s, ms = rest.split(".")
    return int(h) * 3_600_000 + int(m) * 60_000 + int(s) * 1_000 + int(ms)


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


class ChampionshipPredictionFrame(F1Model):
    """Pre-race standings and predicted end-of-race standings."""

    drivers: dict[str, DriverPrediction]
    teams: dict[str, TeamPrediction]


# ── Streams ───────────────────────────────────────────


class DriverPredictionStream(F1Stream):
    SCHEMA: ClassVar[dict[str, pl.DataType]] = {
        "timestamp": pl.Duration("ms"),
        "car_number": pl.Utf8(),
        "predicted_position": pl.UInt8(),
        "predicted_points": pl.Float64(),
    }

    @classmethod
    def _extract_rows(
        cls, timestamp_ms: int, data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        return [
            {
                "timestamp": timestamp_ms,
                "car_number": car_number,
                "predicted_position": update.get("PredictedPosition"),
                "predicted_points": update.get("PredictedPoints"),
            }
            for car_number, update in data.get("Drivers", {}).items()
        ]


class TeamPredictionStream(F1Stream):
    SCHEMA: ClassVar[dict[str, pl.DataType]] = {
        "timestamp": pl.Duration("ms"),
        "team_key": pl.Utf8(),
        "predicted_position": pl.UInt8(),
        "predicted_points": pl.Float64(),
    }

    @classmethod
    def _extract_rows(
        cls, timestamp_ms: int, data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        return [
            {
                "timestamp": timestamp_ms,
                "team_key": team_key,
                "predicted_position": update.get("PredictedPosition"),
                "predicted_points": update.get("PredictedPoints"),
            }
            for team_key, update in data.get("Teams", {}).items()
        ]


# ── Container ─────────────────────────────────────────


class ChampionshipPrediction(F1DataContainer):
    """Championship predictions — keyframe + driver/team streams.

    Both streams share the same source file. The container splits
    the entries into separate DataFrames.
    """

    KEYFRAME_FILE: ClassVar[str | None] = "ChampionshipPrediction.json"
    STREAM_FILE: ClassVar[str | None] = "ChampionshipPrediction.jsonStream"

    keyframe: ChampionshipPredictionFrame | None = None
    drivers: DriverPredictionStream | None = None
    teams: TeamPredictionStream | None = None

    @model_validator(mode="before")
    @classmethod
    def _split_stream(cls, raw: Any) -> dict[str, Any]:
        if not isinstance(raw, dict):
            return raw

        result: dict[str, Any] = {}

        if "keyframe" in raw and raw["keyframe"] is not None:
            result["keyframe"] = raw["keyframe"]

        entries = raw.get("stream")
        if entries is not None:
            # Same entries fed to both stream classes
            result["drivers"] = entries
            result["teams"] = entries

        return result
