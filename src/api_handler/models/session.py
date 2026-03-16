from typing import ClassVar, override
from datetime import datetime, timedelta
from enum import Enum
from pydantic import BaseModel, Field, ConfigDict, field_validator
from pydantic.alias_generators import to_pascal


class SessionType(str, Enum):
    PRACTICE = "Practice"
    QUALIFYING = "Qualifying"
    RACE = "Race"


class SessionSubType(str, Enum):
    PRACTICE_1 = "Practice 1"
    PRACTICE_2 = "Practice 2"
    PRACTICE_3 = "Practice 3"
    QUALIFYING = "Qualifying"
    SPRINT_SHOOTOUT = "Sprint Shootout"
    SPRINT = "Sprint"
    RACE = "Race"

    @override
    def __str__(self) -> str:
        return self.value


class Session(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(populate_by_name=True, alias_generator=to_pascal)

    key: int
    type: SessionType | None
    number: int | None = None
    sub_type: SessionSubType | None = Field(alias="Name", default=None)
    start_date: datetime
    end_date: datetime
    gmt_offset: timedelta
    path: str | None = None

    @field_validator("gmt_offset", mode="before")
    @classmethod
    def parse_gmt_offset(cls, v: str) -> timedelta:
        negative = v.startswith("-")
        v = v.lstrip("-")
        h, m, s = map(int, v.split(":"))
        td = timedelta(hours=h, minutes=m, seconds=s)
        return -td if negative else td

    @property
    def folder_name(self) -> str:
        if self.path is None:
            raise ValueError("Session has no path")
        return self.path.split("/")[2]