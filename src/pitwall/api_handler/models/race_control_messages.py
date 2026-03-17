from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Annotated, Literal, override

from pydantic import Discriminator, Field, Tag

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
    utc: datetime = Field(alias="Utc")
    lap: int | None = Field(alias="Lap", default = None) # Optional because of non-race flags
    message: str = Field(alias="Message")


class SafetyCarMessage(RaceControlBase):
    category: Literal["SafetyCar"] = Field(alias="Category")
    status: SafetyCarStatus = Field(alias="Status")
    mode: SafetyCarMode = Field(alias="Mode")


class FlagMessage(RaceControlBase):
    category: Literal["Flag"] = Field(alias="Category")
    flag: FlagType = Field(alias="Flag")
    scope: FlagScope = Field(alias="Scope")
    sector: int | None = Field(None, alias="Sector")
    racing_number: str | None = Field(None, alias="RacingNumber")


class DrsMessage(RaceControlBase):
    category: Literal["Drs"] = Field(alias="Category")
    status: DrsStatus = Field(alias="Status")
class OtherMessage(RaceControlBase):
    category: Literal["Other"] = Field(alias="Category")


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
    messages: list[RaceControlMessage] = Field(alias="Messages")