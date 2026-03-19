from collections.abc import Iterator
from enum import StrEnum

from pydantic import Field

from pitwall.api_handler.models.base import F1Model


class TyreCompound(StrEnum):
    HARD = "HARD"
    MEDIUM = "MEDIUM"
    SOFT = "SOFT"
    # TODO: Find out how these actually code
    INTERMEDIATE = "INTERMEDIATE"
    WET = "WET"


class TyreInfo(F1Model):
    compound: TyreCompound = Field(alias="Compound")
    new: bool = Field(alias="New")


class CurrentTyres(F1Model):
    tyres: dict[str, TyreInfo]

    def __getitem__(self, car_number: str) -> TyreInfo:
        return self.tyres[car_number]

    def items(self) -> Iterator[tuple[str, TyreInfo]]:
        return iter(self.tyres.items())
