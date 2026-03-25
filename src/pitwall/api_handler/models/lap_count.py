from typing import ClassVar, override

import polars as pl
from pydantic import JsonValue, model_validator

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

    @override
    @classmethod
    def _extract_rows(
        cls, timestamp_ms: int, data: dict[str, JsonValue]
    ) -> list[dict[str, JsonValue]]:
        return [
            {
                "lap": data.get("CurrentLap"),
                "timestamp": timestamp_ms,
            }
        ]

    @model_validator(mode="before")
    @classmethod
    def _from_entries(
        cls, raw: list[dict[str, JsonValue]] | dict[str, object]
    ) -> dict[str, object]:
        if not isinstance(raw, list) or not raw:
            return raw if isinstance(raw, dict) else {}

        total_laps: int = 0
        for entry in raw:
            entry_data = entry.get("Data")
            if isinstance(entry_data, dict):
                tl = entry_data.get("TotalLaps")
                if isinstance(tl, int):
                    total_laps = tl

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
