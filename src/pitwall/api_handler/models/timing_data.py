import re
from datetime import timedelta
from typing import Annotated

from pydantic import BeforeValidator, Field
from pydantic.functional_validators import field_validator

from pitwall.api_handler.models.base import F1Model


def parse_lap_time(value: str) -> timedelta:
    """Parse F1 lap time string like '1:26.933' or '26.933' into timedelta."""
    match = re.fullmatch(r"(?:(\d+):)?(\d+)\.(\d+)", value)
    if not match:
        raise ValueError(f"Invalid lap time: {value!r}")
    minutes = int(match.group(1) or 0)
    seconds = int(match.group(2))
    millis = int(match.group(3).ljust(3, "0")[:3])
    return timedelta(minutes=minutes, seconds=seconds, milliseconds=millis)


LapTime = Annotated[timedelta, BeforeValidator(parse_lap_time)]


class LastLapTime(F1Model):
    value: LapTime = Field(alias="Value")
    # TODO: Figure out what status maps to
    status: int = Field(alias="Status")
    overall_fastest: bool = Field(alias="OverallFastest")
    perosnal_fastest: bool = Field(alias="PersonalFastest")


class BestLapTime(F1Model):
    value: LapTime = Field(alias="Value")
    lap: int = Field(alias="Lap")


class SpeedTrap(F1Model):
    value: str = Field(alias="Value")
    status: int = Field(alias="Status")
    overall_fastest: bool = Field(alias="OverallFastest")
    personal_fastest: bool = Field(alias="PersonalFastest")


class Speeds(F1Model):
    # TODO: Figure out what i1, i2, fl, and st are
    i1: SpeedTrap = Field(alias="I1")
    i2: SpeedTrap = Field(alias="I2")
    fl: SpeedTrap = Field(alias="FL")
    st: SpeedTrap = Field(alias="ST")


class Segment(F1Model):
    # TODO: Figure out how status encodes
    status: int = Field(alias="Status")


class Sector(F1Model):
    stopped: bool = Field(alias="Stopped")
    previous_value: float = Field(alias="PreviousValue")
    segments: list[Segment] = Field(alias="Segments")
    value: float = Field(alias="Value")
    # TODO: Figure out how status encodes
    status: int = Field(alias="Status")
    overall_fastest: bool = Field(alias="OverallFastest")
    personal_fastest: bool = Field(alias="PersonalFastest")


class IntervalData(F1Model):
    value: str = Field(alias="Value")
    catching: bool = Field(alias="Catching")


class TimingLine(F1Model):
    racing_number: str = Field(alias="RacingNumber")
    position: str = Field(alias="Position")
    line: int = Field(alias="Line")
    show_position: bool = Field(alias="ShowPosition")

    # Practice fields
    time_diff_to_fastest: float | int | None = Field(None, alias="TimeDiffToFastest")
    time_diff_to_position_ahead: float | int | None = Field(
        None, alias="TimeDiffToPositionAhead"
    )

    # Race fields
    gap_to_leader: float | int | None = Field(None, alias="GapToLeader")
    interval_to_position_ahead: IntervalData | None = Field(
        None, alias="IntervalToPositionAhead"
    )

    # Common
    retired: bool = Field(alias="Retired")
    in_pit: bool = Field(alias="InPit")
    pit_out: bool = Field(alias="PitOut")
    stopped: bool = Field(alias="Stopped")
    status: int = Field(alias="Status")
    number_of_laps: int = Field(alias="NumberOfLaps")
    number_of_pit_stops: int = Field(alias="NumberOfPitStops")
    sectors: list[Sector] = Field(alias="Sectors")
    speeds: Speeds = Field(alias="Speeds")
    best_lap_time: BestLapTime = Field(alias="BestLapTime")
    last_lap_time: LastLapTime = Field(alias="LastLapTime")

    @field_validator(
        "time_diff_to_fastest",
        "time_diff_to_position_ahead",
        "gap_to_leader",
        mode="before",
    )
    @classmethod
    def parse_gap(cls, v: object) -> float | int | None:
        if not isinstance(v, str) or v == "":
            return None
        cleaned = v.lstrip("+")
        if cleaned.endswith("L"):
            try:
                return -int(cleaned[:-1])
            except ValueError:
                return None
        try:
            return float(cleaned)
        except ValueError:
            return None


class TimingDataF1(F1Model):
    lines: dict[str, TimingLine] = Field(alias="Lines")
    withheld: bool = Field(alias="Withheld")
