from collections.abc import Iterator
from enum import StrEnum
from typing import ClassVar, override

import polars as pl
from pydantic import Field, JsonValue

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


_VALID_COMPOUNDS = {e.value for e in TyreCompound}


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
        cls, timestamp_ms: int, data: dict[str, JsonValue]
    ) -> list[dict[str, JsonValue]]:
        rows: list[dict[str, JsonValue]] = []
        tyres = cls._as_dict(data.get("Tyres"))

        for racing_number, tyre_data in tyres.items():
            if not isinstance(tyre_data, dict):
                continue

            compound_raw = tyre_data.get("Compound")
            compound = (
                compound_raw
                if isinstance(compound_raw, str) and compound_raw in _VALID_COMPOUNDS
                else "UNKNOWN"
            )
            rows.append(
                {
                    "timestamp": timestamp_ms,
                    "racing_number": racing_number,
                    "compound": compound,
                    "new": tyre_data.get("New", False),
                }
            )
        return rows


class CurrentTyres(F1DataContainer):
    KEYFRAME_FILE: ClassVar[str | None] = "CurrentTyres.json"
    STREAM_FILE: ClassVar[str | None] = "CurrentTyres.jsonStream"

    keyframe: CurrentTyresKeyframe
    stream: CurrentTyresStream
