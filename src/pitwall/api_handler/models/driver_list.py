from collections.abc import ItemsView
from typing import ClassVar, override

import polars as pl
from pydantic import Field, JsonValue, model_validator

from .base import F1DataContainer, F1Model, F1Stream


class DriverInfo(F1Model):
    """Static driver information from the keyframe."""

    racing_number: int
    broadcast_name: str
    full_name: str
    tla: str
    line: int
    team_name: str
    team_colour: str
    first_name: str
    last_name: str
    reference: str
    headshot_url: str | None = Field(default=None)
    public_id_right: str | None = Field(default=None)
    country_code: str | None = Field(default=None)

    @property
    def team_color(self) -> str:
        return self.team_colour


class DriverListKeyframe(F1Model):
    """All drivers keyed by car number."""

    drivers: dict[str, DriverInfo]

    def __getitem__(self, car_number: str) -> DriverInfo:
        return self.drivers[car_number]

    def items(self) -> ItemsView[str, DriverInfo]:
        return self.drivers.items()

    @model_validator(mode="before")
    @classmethod
    def _wrap(cls, data: dict[str, JsonValue]) -> dict[str, JsonValue]:
        if "drivers" not in data:
            return {"drivers": data}
        return data


class DriverListStream(F1Stream):
    """Position (Line) updates per car over time."""

    SCHEMA: ClassVar[dict[str, pl.DataType]] = {
        "timestamp": pl.Duration("ms"),
        "car_number": pl.Utf8(),
        "line": pl.UInt8(),
    }

    @override
    @classmethod
    def _extract_rows(
        cls, timestamp_ms: int, data: dict[str, JsonValue]
    ) -> list[dict[str, JsonValue]]:
        return [
            {
                "timestamp": timestamp_ms,
                "car_number": car_number,
                "line": update.get("Line"),
            }
            for car_number, update in data.items()
            if isinstance(update, dict) and "Line" in update
        ]


class DriverList(F1DataContainer):
    """Driver list — static info keyframe + position stream.

    The keyframe contains full driver metadata (name, team, headshot).
    The stream tracks Line (display position) changes over time.
    """

    KEYFRAME_FILE: ClassVar[str | None] = "DriverList.json"
    STREAM_FILE: ClassVar[str | None] = "DriverList.jsonStream"

    keyframe: DriverListKeyframe | None = None
    stream: DriverListStream | None = None
