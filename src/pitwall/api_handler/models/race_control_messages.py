from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Annotated, ClassVar, Literal, override

from pydantic import ConfigDict, Discriminator, Field, Tag
from pydantic.alias_generators import to_pascal

from pitwall.api_handler.models.base import F1Model


class FlagType(StrEnum):
    BLUE = "BLUE"
    YELLOW = "YELLOW"
    DOUBLE_YELLOW = "DOUBLE YELLOW"
    RED = "RED"
    GREEN = "GREEN"
    CLEAR = "CLEAR"
    CHEQUERED = "CHEQUERED"
    BLACK_AND_WHITE = "BLACK AND WHITE"
    BLACK_AND_ORANGE = "BLACK AND ORANGE"


class FlagScope(StrEnum):
    DRIVER = "Driver"
    SECTOR = "Sector"
    TRACK = "Track"


class SafetyCarStatus(StrEnum):
    DEPLOYED = "DEPLOYED"
    ENDING = "ENDING"
    IN_THIS_LAP = "IN THIS LAP"


class DrsStatus(StrEnum):
    DISABLED = "DISABLED"
    ENABLED = "ENABLED"


class SafetyCarMode(StrEnum):
    SAFETY_CAR = "SAFETY CAR"
    VIRTUAL_SAFETY_CAR = "VIRTUAL SAFETY CAR"


class RcmCategory(StrEnum):
    FLAG = "Flag"
    SAFETY_CAR = "SafetyCar"
    OTHER = "Other"


class RaceControlBase(F1Model):
    utc: datetime
    lap: int | None = Field(default=None)  # Optional because of non-race flags
    message: str


class SafetyCarMessage(RaceControlBase):
    category: Literal["SafetyCar"]
    status: SafetyCarStatus
    mode: SafetyCarMode


class FlagMessage(RaceControlBase):
    category: Literal["Flag"]
    flag: FlagType
    scope: FlagScope
    sector: int | None
    racing_number: str | None


class DrsMessage(RaceControlBase):
    category: Literal["Drs"]
    status: DrsStatus


class OtherMessage(RaceControlBase):
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
    messages: list[RaceControlMessage]
