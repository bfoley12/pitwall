from datetime import datetime
from typing import ClassVar, override

import polars as pl
from pydantic import JsonValue, model_validator

from pitwall.api_handler.models.base import (
    F1DataContainer,
    F1Frame,
    F1Model,
    F1Stream,
    ParsedValue,
)
from pitwall.api_handler.registry import register


class Overtake(F1Model):
    timestamp: datetime
    count: int


class DriverOvertakes(F1Model):
    racing_number: str
    overtakes: list[Overtake]


class OvertakeSeriesKeyframe(F1Frame):
    overtakes: list[DriverOvertakes]

    @model_validator(mode="before")
    @classmethod
    def _unpack_overtakes(cls, v: dict[str, JsonValue]) -> dict[str, JsonValue]:
        raw = cls._as_dict(v.get("Overtakes", {}))
        return {
            "overtakes": [
                {"racing_number": num, "overtakes": events}
                for num, events in raw.items()
            ]
        }


class OvertakeSeriesStream(F1Stream):
    SCHEMA: ClassVar[dict[str, pl.DataType]] = {
        "timestamp": pl.Duration("ms"),
        "utc": pl.Datetime("ms"),
        "racing_number": pl.UInt8(),
        # TODO: Understand what "count" refers to?
        "count": pl.UInt8(),
    }

    @override
    @classmethod
    def _extract_rows(
        cls, timestamp_ms: int, data: dict[str, JsonValue]
    ) -> list[dict[str, ParsedValue]]:
        rows: list[dict[str, ParsedValue]] = []
        for racing_number, events in cls._as_dict(data.get("Overtakes", {})).items():
            for ot in cls._as_dict(events).values():
                ot_dict = cls._as_dict(ot)
                utc_raw = ot_dict.get("Timestamp")
                utc = (
                    cls._parse_utc(cls._as_str(utc_raw))
                    if isinstance(utc_raw, str)
                    else None
                )
                rows.append(
                    {
                        "timestamp": timestamp_ms,
                        "utc": utc,
                        "racing_number": racing_number,
                        "count": ot_dict.get("count"),
                    }
                )
        return rows


@register
class OvertakeSeries(F1DataContainer[OvertakeSeriesKeyframe, OvertakeSeriesStream]):
    KEYFRAME_FILE: ClassVar[str | None] = "OvertakeSeries.json"
    STREAM_FILE: ClassVar[str | None] = "OvertakeSeries.jsonStream"

    keyframe: OvertakeSeriesKeyframe
    stream: OvertakeSeriesStream
