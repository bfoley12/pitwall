from __future__ import annotations

from typing import Any, ClassVar

import polars as pl
from pydantic import ConfigDict, model_validator

from .base import F1Model
from .championship_prediction import parse_timestamp


class LapCount(F1Model):
    """Race lap counter from LapCount.jsonStream.

    Attributes:
        total_laps: Total laps in the race (e.g. 56).
        current_lap: Final lap reached.
        laps: Mapping of lap number to the session elapsed time
            when the leader crossed the start/finish line.
    """

    model_config: ClassVar[ConfigDict] = ConfigDict(arbitrary_types_allowed=True)

    total_laps: int
    current_lap: int
    laps: pl.DataFrame

    @model_validator(mode="before")
    @classmethod
    def from_stream(cls, entries: list[dict[str, Any]]) -> dict[str, Any]:
        """Build from decoded LapCount.jsonStream entries."""
        total_laps: int = 0
        rows: list[dict[str, int]] = []

        for entry in entries:
            data: dict[str, Any] = entry["Data"]
            if "TotalLaps" in data:
                total_laps = data["TotalLaps"]
            rows.append(
                {
                    "lap": data["CurrentLap"],
                    "timestamp": parse_timestamp(entry["Timestamp"]),
                }
            )

        laps = pl.DataFrame(
            rows,
            schema={"lap": pl.UInt8(), "timestamp": pl.Duration("ms")},
        )

        current_lap = laps["lap"].max() if len(laps) > 0 else 0

        return {
            "total_laps": total_laps,
            "current_lap": current_lap,
            "laps": laps,
        }
