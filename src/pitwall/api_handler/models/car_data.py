from typing import ClassVar, override

import polars as pl
from pydantic import Field, JsonValue

from pitwall.api_handler.models.base import (
    F1DataContainer,
    F1Frame,
    F1Model,
    F1Stream,
    ParsedValue,
)
from pitwall.api_handler.registry import register

# TODO: Decode Keyframe too (not actually informative, but just for completeness)
# TODO: If F1 releases 2026 data (active aero and battery) reassess this labeling
# - Might need to version if channels change
CHANNEL_MAP: dict[str, str] = {
    "0": "rpm",
    "2": "speed",
    "3": "gear",
    "4": "throttle",
    "5": "brake",
    "45": "drs",
}


class CarDataChannels(F1Model):
    rpm: int = Field(0, alias="0")
    speed: int = Field(0, alias="2")
    gear: int = Field(0, alias="3")
    throttle: int = Field(0, alias="4")
    brake: int = Field(0, alias="5")
    drs: int | None = Field(None, alias="45")


class CarDataEntry(F1Model):
    channels: CarDataChannels


class CarDataSnapshot(F1Model):
    utc: str
    cars: dict[str, CarDataEntry]


class CarDataKeyframe(F1Frame):
    entries: list[CarDataSnapshot]

    def to_rows(self) -> list[dict[str, object]]:
        return [
            {
                "utc": snapshot.utc,
                "car_number": car_num,
                **snapshot.cars[car_num].channels.model_dump(),
            }
            for snapshot in self.entries
            for car_num in snapshot.cars
        ]


class CarDataStream(F1Stream):
    SCHEMA: ClassVar[dict[str, pl.DataType]] = {
        "timestamp": pl.Duration("ms"),
        "utc": pl.Datetime(),
        "racing_number": pl.UInt8(),
        "rpm": pl.UInt16(),
        "speed": pl.UInt16(),
        "gear": pl.UInt8(),
        "throttle": pl.Float16(),
        # Only values from 2023 Abu Dhabi: [0, 100, 104]
        "brake": pl.UInt8(),
        # Odd values from 2023 Abu Dhabi (only one tested so far) - [0, 1, 2, 3, 8, 10, 12, 14]
        "drs": pl.UInt8(),
    }

    @override
    @classmethod
    def _extract_rows(
        cls, timestamp_ms: int, data: dict[str, JsonValue]
    ) -> list[dict[str, ParsedValue]]:
        rows: list[dict[str, ParsedValue]] = []
        utc_raw = data.get("Utc")
        utc = cls._parse_utc(cls._as_str(utc_raw)) if isinstance(utc_raw, str) else None
        cars = cls._as_dict(data.get("Cars"))

        for racing_num, car in cars.items():
            if not isinstance(car, dict):
                continue
            ch = cls._as_dict(car.get("Channels"))
            rows.append(
                {
                    "timestamp": timestamp_ms,
                    "utc": utc,
                    "racing_number": racing_num,
                    "rpm": ch.get("0", 0),
                    "speed": ch.get("2", 0),
                    "gear": ch.get("3", 0),
                    "throttle": ch.get("4", 0),
                    "brake": ch.get("5", 0),
                    "drs": ch.get("45"),
                }
            )

        return rows


@register
class CarData(F1DataContainer[CarDataKeyframe, CarDataStream]):
    KEYFRAME_FILE: ClassVar[str | None] = "CarData.z.json"
    STREAM_FILE: ClassVar[str | None] = "CarData.z.jsonStream"

    keyframe: CarDataKeyframe
    stream: CarDataStream
