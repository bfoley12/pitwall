from datetime import timedelta
from typing import ClassVar

from pydantic import ConfigDict, Field, field_validator, model_validator
from pydantic.alias_generators import to_pascal

from .base import F1Model
from .session import SessionType
from .timing_data import parse_lap_time

# TODO: Unify with timing_data? I modeled it by file, but there is significant overlap


# TODO: In the case of a DNS (ie lando (RacingNumber 1) at 2026 Shanghai Race), a lot of these models collapse to {"value": ""}
# We could drop them (I think that decision should be made above this level)
class RankedValue(F1Model):
    position: int | None = Field(default=None)
    value: float | None = Field(default=None)

    @field_validator("value", mode="before")
    @classmethod
    def _parse_value(cls, v: str | float | int | None) -> float | None:
        if v == "" or v is None:
            return None
        return float(v)


class PersonalBestLapTime(F1Model):
    lap: int | None = Field(default=None)
    position: int | None = Field(default=None)
    time: timedelta | None = Field(alias="Value", default=None)

    @field_validator("time", mode="before")
    @classmethod
    def _parse_time(cls, v: timedelta | str | None) -> timedelta | None:
        if v == "" or v is None:
            return None
        if isinstance(v, str):
            return parse_lap_time(v)
        return v


class BestSpeeds(F1Model):
    i1: RankedValue = Field(alias="I1")
    i2: RankedValue = Field(alias="I2")
    fl: RankedValue = Field(alias="FL")
    st: RankedValue = Field(alias="ST")


class TimingStatsLine(F1Model):
    line: int
    racing_number: str
    personal_best_lap_time: PersonalBestLapTime
    best_sectors: list[RankedValue]
    best_speeds: BestSpeeds


class TimingStats(F1Model):
    withheld: bool
    session_type: SessionType
    lines: list[TimingStatsLine]

    @model_validator(mode="before")
    @classmethod
    def _lines_to_list(cls, data: dict[str, object]) -> dict[str, object]:
        lines = data.get("Lines", {})
        if isinstance(lines, dict):
            data["Lines"] = list(lines.values())
        return data
