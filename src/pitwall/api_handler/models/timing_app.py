from datetime import timedelta
from typing import ClassVar

from pydantic import ConfigDict, Field, field_validator, model_validator
from pydantic.alias_generators import to_pascal

from pitwall.api_handler.models.tyres import TyreCompound

from .base import F1Model
from .timing_data import parse_lap_time


class Stint(F1Model):
    model_config: ClassVar[ConfigDict] = ConfigDict(
        populate_by_name=True, alias_generator=to_pascal
    )

    # TODO: Clarify: these may be none when the car fails to finish the lap(?)
    lap_time: timedelta | None = Field(default=None)
    lap_number: int | None = Field(default=None)
    # TODO: Clarify what LapFlags denotes
    lap_flags: int
    compound: TyreCompound
    new: bool
    tyres_not_changed: int
    # TODO: Clarify that this is stint laps + start laps vs just stint laps
    total_laps: int
    # TODO: Clarify that this is the number of laps on tyre before the current stint
    start_laps: int

    @field_validator("lap_time", mode="before")
    @classmethod
    def _parse_time(cls, v: timedelta | str | None) -> timedelta | None:
        if v == "" or v is None:
            return None
        if isinstance(v, str):
            return parse_lap_time(v)
        return v

class TimingAppData(F1Model):
    model_config: ClassVar[ConfigDict] = ConfigDict(
        populate_by_name=True, alias_generator=to_pascal
    )

    racing_number: int
    line: int
    # May be None outside of race setting
    grid_pos: int | None = Field(default=None)
    stints: list[Stint]


class TimingApp(F1Model):
    model_config: ClassVar[ConfigDict] = ConfigDict(
        populate_by_name=True, alias_generator=to_pascal
    )

    lines: list[TimingAppData]

    @model_validator(mode="before")
    @classmethod
    def _lines_to_list(cls, data: dict[str, object]) -> dict[str, object]:
        lines = data.get("Lines", {})
        if isinstance(lines, dict):
            data["Lines"] = list(lines.values())
        return data
