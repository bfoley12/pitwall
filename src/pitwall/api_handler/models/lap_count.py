from typing import Any, ClassVar

import polars as pl
from pydantic import model_validator

from .base import F1DataContainer, F1Frame, F1Stream


class LapCountStream(F1Stream):
    """Lap counter stream — leader S/F crossings.

    frame columns: lap (UInt8), timestamp (Duration[ms])
    """

    SCHEMA: ClassVar[dict[str, pl.DataType]] = {
        "lap": pl.UInt8(),
        "timestamp": pl.Duration("ms"),
    }

    total_laps: int = 0
    current_lap: int = 0

    @classmethod
    def _extract_rows(
        cls, timestamp_ms: int, data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        return [
            {
                "lap": data["CurrentLap"],
                "timestamp": timestamp_ms,
            }
        ]

    @model_validator(mode="before")
    @classmethod
    def _from_entries(cls, raw: Any) -> Any:
        if not isinstance(raw, list) or not raw:
            return raw

        total_laps: int = 0
        for entry in raw:
            if "TotalLaps" in entry["Data"]:
                total_laps = entry["Data"]["TotalLaps"]

        frame = cls._build_dataframe(raw)
        current_lap = frame["lap"].max() if len(frame) > 0 else 0

        return {
            "data": frame,
            "total_laps": total_laps,
            "current_lap": current_lap,
        }


class LapCountKeyframe(F1Frame):
    current_lap: int
    total_laps: int


class LapCount(F1DataContainer):
    """Race lap counter.

    keyframe: Final state — current_lap and total_laps.
    stream.frame: Lap number + timestamp of each leader S/F crossing.
    stream.total_laps: Total laps in the race.
    stream.current_lap: Final lap reached.
    """

    KEYFRAME_FILE: ClassVar[str | None] = "LapCount.json"
    STREAM_FILE: ClassVar[str | None] = "LapCount.jsonStream"

    keyframe: LapCountKeyframe | None = None
    stream: LapCountStream | None = None
