from typing import Any, ClassVar

import polars as pl

from pitwall.api_handler.models.base import F1DataContainer, F1Frame, F1Model, F1Stream
from pitwall.api_handler.models.current_tyres import TyreCompound


class TyreStintInfo(F1Model):
    compound: TyreCompound
    new: bool
    tyres_not_changed: int
    total_laps: int
    start_laps: int


class TyreStintSeriesStream(F1Stream):
    SCHEMA: ClassVar[dict[str, pl.DataType]] = {
        "timestamp": pl.Duration("ms"),
        "car_number": pl.UInt8(),
        "stint_index": pl.UInt8(),
        "compound": pl.Utf8(),
        "new": pl.Boolean(),
        "tyres_not_changed": pl.UInt8(),
        "total_laps": pl.UInt16(),
        "start_laps": pl.UInt16(),
    }

    @classmethod
    def _extract_rows(
        cls, timestamp_ms: int, data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        stints = data.get("Stints", {})
        for car_num, car_stints in stints.items():
            if isinstance(car_stints, dict):
                for stint_idx, fields in car_stints.items():
                    rows.append(
                        {
                            "timestamp": timestamp_ms,
                            "car_number": int(car_num),
                            "stint_index": int(stint_idx),
                            "compound": fields.get("Compound"),
                            "new": fields.get("New") == "true"
                            if fields.get("New") is not None
                            else None,
                            "tyres_not_changed": fields.get("TyresNotChanged"),
                            "total_laps": fields.get("TotalLaps"),
                            "start_laps": fields.get("StartLaps"),
                        }
                    )
        return rows


class TyreStintSeriesKeyframe(F1Frame):
    stints: dict[int, list[TyreStintInfo]]


class TyreStintSeries(F1DataContainer):
    KEYFRAME_FILE: ClassVar[str | None] = "TyreStintSeries.json"
    STREAM_FILE: ClassVar[str | None] = "TyreStintSeries.jsonStream"

    keyframe: TyreStintSeriesKeyframe
    stream: TyreStintSeriesStream
