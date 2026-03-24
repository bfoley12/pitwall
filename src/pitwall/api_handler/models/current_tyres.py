from collections.abc import Iterator
from enum import StrEnum
from typing import Any, ClassVar, override

import polars as pl
from pydantic import Field

from pitwall.api_handler.models.base import F1DataContainer, F1Frame, F1Model, F1Stream


class TyreCompound(StrEnum):
    HARD = "HARD"
    MEDIUM = "MEDIUM"
    SOFT = "SOFT"
    # TODO: Find out how these actually code
    INTERMEDIATE = "INTERMEDIATE"
    WET = "WET"
    # TODO: Find way to disambiguate this. Seen in 2023 Abu Dhabi GP Race
    UNKNOWN = "UNKNOWN"


class TyreInfo(F1Model):
    compound: TyreCompound = Field(alias="Compound")
    new: bool = Field(alias="New")


class CurrentTyresKeyframe(F1Frame):
    tyres: dict[str, TyreInfo]

    def __getitem__(self, car_number: str) -> TyreInfo:
        return self.tyres[car_number]

    def items(self) -> Iterator[tuple[str, TyreInfo]]:
        return iter(self.tyres.items())


class CurrentTyresStream(F1Stream):
    SCHEMA: ClassVar[dict[str, pl.DataType]] = {
        "timestamp": pl.Duration("ms"),
        "racing_number": pl.UInt8(),
        "compound": pl.Categorical(),
        "new": pl.Boolean(),
    }

    @classmethod
    @override
    def _extract_rows(
        cls, timestamp_ms: int, data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []

        for racing_number, tyre_data in data.get("Tyres", {}).items():
            if not isinstance(tyre_data, dict):
                continue
            rows.append(
                {
                    "timestamp": timestamp_ms,
                    "racing_number": racing_number,
                    "compound": TyreCompound(tyre_data.get("Compound", "UNKNOWN")),
                    "new": tyre_data.get("New", False),
                }
            )
        return rows


class CurrentTyres(F1DataContainer):
    KEYFRAME_FILE: ClassVar[str | None] = "CurrentTyres.json"
    STREAM_FILE: ClassVar[str | None] = "CurrentTyres.jsonStream"

    keyframe: CurrentTyresKeyframe
    stream: CurrentTyresStream
