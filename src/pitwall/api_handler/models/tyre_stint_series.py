from typing import ClassVar, override

import polars as pl
from pydantic import JsonValue

from pitwall.api_handler.models.base import F1DataContainer, F1Frame, F1Model, F1Stream
from pitwall.api_handler.models.current_tyres import TyreCompound
from pitwall.api_handler.registry import register


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

    @override
    @classmethod
    def _extract_rows(
        cls, timestamp_ms: int, data: dict[str, JsonValue]
    ) -> list[dict[str, JsonValue]]:
        rows: list[dict[str, JsonValue]] = []
        stints = cls._as_dict(data.get("Stints"))

        for car_num, car_stints in stints.items():
            if not isinstance(car_stints, dict):
                continue
            for stint_idx, fields in car_stints.items():
                if not isinstance(fields, dict):
                    continue
                new_raw = fields.get("New")
                rows.append(
                    {
                        "timestamp": timestamp_ms,
                        "car_number": int(car_num),
                        "stint_index": int(stint_idx),
                        "compound": fields.get("Compound"),
                        "new": new_raw == "true" if isinstance(new_raw, str) else None,
                        "tyres_not_changed": fields.get("TyresNotChanged"),
                        "total_laps": fields.get("TotalLaps"),
                        "start_laps": fields.get("StartLaps"),
                    }
                )

        return rows


class TyreStintSeriesKeyframe(F1Frame):
    stints: dict[int, list[TyreStintInfo]]


@register
class TyreStintSeries(F1DataContainer[TyreStintSeriesKeyframe, TyreStintSeriesStream]):
    KEYFRAME_FILE: ClassVar[str | None] = "TyreStintSeries.json"
    STREAM_FILE: ClassVar[str | None] = "TyreStintSeries.jsonStream"

    keyframe: TyreStintSeriesKeyframe
    stream: TyreStintSeriesStream
