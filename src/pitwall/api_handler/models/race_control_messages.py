from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Annotated, ClassVar, Literal, override

from pydantic import ConfigDict, Discriminator, Field, Tag
from pydantic.alias_generators import to_pascal

from pitwall.api_handler.models.base import F1Model


class FlexibleStrEnum(StrEnum):
    """StrEnum that accepts unknown values instead of raising."""

    @override
    @classmethod
    def _missing_(cls, value: object) -> StrEnum | None:
        if isinstance(value, str):
            obj = str.__new__(cls, value)
            obj._name_ = value
            obj._value_ = value
            return obj
        return None


class FlagType(FlexibleStrEnum):
    BLUE = "BLUE"
    YELLOW = "YELLOW"
    DOUBLE_YELLOW = "DOUBLE YELLOW"
    RED = "RED"
    GREEN = "GREEN"
    CLEAR = "CLEAR"
    CHEQUERED = "CHEQUERED"
    BLACK_AND_WHITE = "BLACK AND WHITE"
    BLACK_AND_ORANGE = "BLACK AND ORANGE"


class FlagScope(FlexibleStrEnum):
    DRIVER = "Driver"
    SECTOR = "Sector"
    TRACK = "Track"


class SafetyCarStatus(FlexibleStrEnum):
    DEPLOYED = "DEPLOYED"
    ENDING = "ENDING"
    IN_THIS_LAP = "IN THIS LAP"


class DrsStatus(FlexibleStrEnum):
    DISABLED = "DISABLED"
    ENABLED = "ENABLED"


class SafetyCarMode(FlexibleStrEnum):
    SAFETY_CAR = "SAFETY CAR"
    VIRTUAL_SAFETY_CAR = "VIRTUAL SAFETY CAR"


class RcmCategory(FlexibleStrEnum):
    FLAG = "Flag"
    SAFETY_CAR = "SafetyCar"
    OTHER = "Other"


class RaceControlBase(F1Model):
    model_config: ClassVar[ConfigDict] = ConfigDict(
        populate_by_name=True, alias_generator=to_pascal
    )
    utc: datetime
    lap: int | None = Field(default=None)  # Optional because of non-race flags
    message: str


class SafetyCarMessage(RaceControlBase):
    model_config: ClassVar[ConfigDict] = ConfigDict(
        populate_by_name=True, alias_generator=to_pascal
    )
    category: Literal["SafetyCar"]
    status: SafetyCarStatus
    mode: SafetyCarMode


class FlagMessage(RaceControlBase):
    model_config: ClassVar[ConfigDict] = ConfigDict(
        populate_by_name=True, alias_generator=to_pascal
    )
    category: Literal["Flag"]
    flag: FlagType
    scope: FlagScope
    sector: int | None
    racing_number: str | None


class DrsMessage(RaceControlBase):
    model_config: ClassVar[ConfigDict] = ConfigDict(
        populate_by_name=True, alias_generator=to_pascal
    )
    category: Literal["Drs"]
    status: DrsStatus


class OtherMessage(RaceControlBase):
    model_config: ClassVar[ConfigDict] = ConfigDict(
        populate_by_name=True, alias_generator=to_pascal
    )
    category: Literal["Other"]


def _discriminate_rcm(data: dict[str, object]) -> str:
    match data.get("Category"):
        case "SafetyCar":
            return "safety_car"
        case "Flag":
            return "flag"
        case "Drs":
            return "drs"
        case _:
            return "other"


RaceControlMessage = Annotated[
    Annotated[SafetyCarMessage, Tag("safety_car")]
    | Annotated[FlagMessage, Tag("flag")]
    | Annotated[DrsMessage, Tag("drs")]
    | Annotated[OtherMessage, Tag("other")],
    Discriminator(_discriminate_rcm),
]


# TODO: Better parsing of messages - ie decipher potential penalties, etc
# Probably better left to the caller to do, but we can implement that layer later
class RaceControlMessages(F1Model):
    model_config: ClassVar[ConfigDict] = ConfigDict(
        populate_by_name=True, alias_generator=to_pascal
    )
    messages: list[RaceControlMessage]
