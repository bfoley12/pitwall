from datetime import datetime
from enum import StrEnum
from typing import Annotated, ClassVar, Literal, override

import polars as pl
from pydantic import Discriminator, Field, JsonValue, Tag, model_validator

from pitwall.api_handler.registry import register

from .base import F1DataContainer, F1Frame, F1Model, F1Stream


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
    DRS = "Drs"
    OTHER = "Other"


# ── Message types ─────────────────────────────────────


class RaceControlBase(F1Model):
    utc: datetime
    lap: int | None = Field(default=None)
    message: str


class SafetyCarMessage(RaceControlBase):
    category: Literal["SafetyCar"]
    status: SafetyCarStatus
    mode: SafetyCarMode


class FlagMessage(RaceControlBase):
    category: Literal["Flag"]
    flag: FlagType
    scope: FlagScope
    sector: int | None = None
    racing_number: str | None = None


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


# ── Keyframe ──────────────────────────────────────────


class RaceControlMessagesKeyframe(F1Frame):
    messages: list[RaceControlMessage]

    @model_validator(mode="before")
    @classmethod
    def _wrap(cls, data: dict[str, JsonValue]) -> dict[str, JsonValue]:
        if "Messages" in data:
            return {"messages": data["Messages"]}
        return data


# ── Stream ────────────────────────────────────────────


class RaceControlMessagesStream(F1Stream):
    """Race control messages as a time series."""

    SCHEMA: ClassVar[dict[str, pl.DataType]] = {
        "timestamp": pl.Duration("ms"),
        "utc": pl.Utf8(),
        "category": pl.Utf8(),
        "message": pl.Utf8(),
        "lap": pl.UInt8(),
        "flag": pl.Utf8(),
        "scope": pl.Utf8(),
        "sector": pl.UInt8(),
        "racing_number": pl.Utf8(),
        "status": pl.Utf8(),
        "mode": pl.Utf8(),
    }

    @override
    @classmethod
    def _extract_rows(
        cls, timestamp_ms: int, data: dict[str, JsonValue]
    ) -> list[dict[str, JsonValue]]:
        raw_msgs = data.get("Messages")
        if isinstance(raw_msgs, list):
            messages = raw_msgs
        elif isinstance(raw_msgs, dict):
            messages = list(raw_msgs.values())
        else:
            return []

        rows: list[dict[str, JsonValue]] = []
        for msg in messages:
            if not isinstance(msg, dict):
                continue
            rows.append(
                {
                    "timestamp": timestamp_ms,
                    "utc": msg.get("Utc"),
                    "category": msg.get("Category"),
                    "message": msg.get("Message"),
                    "lap": msg.get("Lap"),
                    "flag": msg.get("Flag"),
                    "scope": msg.get("Scope"),
                    "sector": msg.get("Sector"),
                    "racing_number": msg.get("RacingNumber"),
                    "status": msg.get("Status"),
                    "mode": msg.get("Mode"),
                }
            )

        return rows


# ── Container ─────────────────────────────────────────


@register
class RaceControlMessages(F1DataContainer[RaceControlMessagesKeyframe, RaceControlMessagesStream]):
    """Race control messages — flags, penalties, safety car, DRS.

    keyframe: Typed discriminated union of message objects.
    stream.frame: Flat DataFrame of all messages for time-series analysis.
    """

    KEYFRAME_FILE: ClassVar[str | None] = "RaceControlMessages.json"
    STREAM_FILE: ClassVar[str | None] = "RaceControlMessages.jsonStream"

    keyframe: RaceControlMessagesKeyframe
    stream: RaceControlMessagesStream
