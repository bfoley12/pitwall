from typing import ClassVar, override

import polars as pl
from pydantic import JsonValue, model_validator

from pitwall.api_handler.models.base import F1DataContainer, F1Frame, F1Model, F1Stream


class DriverPositions(F1Model):
    racing_number: int
    lap_position: list[int]


class LapSeriesKeyframe(F1Frame):
    driver_positions: dict[int, DriverPositions]

    @model_validator(mode="before")
    @classmethod
    def wrap(cls, data: dict[str, JsonValue]) -> dict[str, JsonValue]:
        return {"DriverPositions": data}


class LapSeriesStream(F1Stream):
    SCHEMA: ClassVar[dict[str, pl.DataType]] = {
        "timestamp": pl.Duration("ms"),
        "racing_number": pl.UInt8(),
        "lap": pl.UInt8(),
        "position": pl.UInt8(),
    }

    @override
    @classmethod
    def _extract_rows(
        cls, timestamp_ms: int, data: dict[str, JsonValue]
    ) -> list[dict[str, JsonValue]]:
        rows: list[dict[str, JsonValue]] = []
        for racing_number, position in data.items():
            if not isinstance(position, dict):
                continue
            lap_position = position.get("LapPosition")
            if not isinstance(lap_position, dict):
                continue
            for lap, pos in lap_position.items():
                rows.append(
                    {
                        "timestamp": timestamp_ms,
                        "racing_number": racing_number,
                        "lap": lap,
                        "position": pos,
                    }
                )
        return rows


class LapSeries(F1DataContainer):
    KEYFRAME_FILE: ClassVar[str | None] = "LapSeries.json"
    STREAM_FILE: ClassVar[str | None] = "LapSeries.jsonStream"

    keyframe: LapSeriesKeyframe
    stream: LapSeriesStream
