from collections.abc import ItemsView
from typing import Any, ClassVar

import polars as pl
from pydantic import model_validator

from pitwall.api_handler.models.current_tyres import TyreCompound
from pitwall.api_handler.models.timing_data import LapTime

from .base import F1DataContainer, F1Frame, F1Model, F1Stream


# Most field duplicated in tyre_stint_series.py: TyreStintInfo
# Leaving for now as a more transparaent wrapper to the api
class StintInfo(F1Model):
    lap_time: LapTime | None = None
    lap_flags: int | None = None
    compound: TyreCompound | None = None
    new: bool | None = None
    tyres_not_changed: int | None = None
    total_laps: int | None = None
    start_laps: int | None = None


class TimingAppLine(F1Model):
    racing_number: str
    line: int
    grid_pos: str
    stints: list[StintInfo]


class TimingAppKeyframe(F1Frame):
    drivers: dict[str, TimingAppLine]

    @model_validator(mode="before")
    @classmethod
    def _unwrap(cls, data: Any) -> Any:
        if isinstance(data, dict) and "Lines" in data:
            return {"drivers": data["Lines"]}
        if isinstance(data, dict) and "drivers" not in data:
            return {"drivers": data}
        return data

    def __getitem__(self, car_number: str) -> TimingAppLine:
        return self.drivers[car_number]

    def items(self) -> ItemsView[str, TimingAppLine]:
        return self.drivers.items()


class TimingAppStream(F1Stream):
    """Tyre stint updates from TimingAppData.jsonStream.

    Each row is a delta for one car's stint. Null fields mean
    "no change from previous value." New stint entries include
    Compound, New, etc. Subsequent updates typically only
    increment TotalLaps.
    """

    SCHEMA: ClassVar[dict[str, pl.DataType]] = {
        "timestamp": pl.Duration("ms"),
        "car_number": pl.Utf8(),
        "stint_number": pl.UInt8(),
        "compound": pl.Utf8(),
        "new": pl.Boolean(),
        "tyres_not_changed": pl.UInt8(),
        "total_laps": pl.UInt16(),
        "start_laps": pl.UInt16(),
        "lap_time": pl.Utf8(),
        "lap_number": pl.UInt16(),
        "lap_flags": pl.UInt8(),
    }

    @classmethod
    def _extract_rows(
        cls, timestamp_ms: int, data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []

        for car_number, car_data in data.get("Lines", {}).items():
            raw_stints = car_data.get("Stints", {})

            # First entry uses a list, subsequent use index-keyed dicts
            if isinstance(raw_stints, list):
                stints = {str(i): s for i, s in enumerate(raw_stints)}
            else:
                stints = raw_stints

            for stint_idx, stint in stints.items():
                if not isinstance(stint, dict):
                    continue
                rows.append(
                    {
                        "timestamp": timestamp_ms,
                        "car_number": car_number,
                        "stint_number": int(stint_idx),
                        "compound": stint.get("Compound"),
                        "new": (stint["New"] == "true" if "New" in stint else None),
                        "tyres_not_changed": (
                            int(stint["TyresNotChanged"])
                            if "TyresNotChanged" in stint
                            else None
                        ),
                        "total_laps": stint.get("TotalLaps"),
                        "start_laps": stint.get("StartLaps"),
                        "lap_time": stint.get("LapTime"),
                        "lap_number": stint.get("LapNumber"),
                        "lap_flags": stint.get("LapFlags"),
                    }
                )

        return rows


class TimingAppData(F1DataContainer):
    """Stint and tyre data from TimingAppData.

    keyframe: Final state from TimingAppData.json.
    stream.frame: Sparse stint deltas over time.
        New stint rows include compound/new/start_laps.
        Subsequent rows typically just increment total_laps.
    """

    KEYFRAME_FILE: ClassVar[str | None] = "TimingAppData.json"
    STREAM_FILE: ClassVar[str | None] = "TimingAppData.jsonStream"

    keyframe: TimingAppKeyframe | None = None
    stream: TimingAppStream | None = None
