from datetime import datetime
from typing import override

from pydantic import Field, model_validator

from pitwall.api_handler.models.base import F1Model

from .circuit import Circuit
from .country import Country
from .session import Session

SESSION_KEYS = {
    "Practice 1": "FP1",
    "Practice 2": "FP2",
    "Practice 3": "FP3",
    "Qualifying": "Q",
    "Sprint Shootout": "SQ",
    "Race": "Race",
}


class Meeting(F1Model):
    key: int = Field(alias="Key")
    code: str = Field(alias="Code")
    number: int = Field(alias="Number")
    location: str = Field(alias="Location")
    official_name: str = Field(alias="OfficialName")
    name: str = Field(alias="Name")
    country: Country = Field(alias="Country")
    circuit: Circuit = Field(alias="Circuit")
    sessions: list[Session] = Field(alias="Sessions")

    @model_validator(mode="after")
    def sort_sessions(self) -> "Meeting":
        self.sessions.sort(key=lambda s: s.start_date)
        return self

    @property
    def fp1(self) -> Session:
        return self._by_type("Practice 1")

    @property
    def fp2(self) -> Session:
        return self._by_type("Practice 2")

    @property
    def fp3(self) -> Session:
        return self._by_type("Practice 3")

    @property
    def q(self) -> Session:
        return self._by_type("Qualifying")

    @property
    def sq(self) -> Session:
        return self._by_type("Sprint Shootout")

    @property
    def sprint(self) -> Session:
        return self._by_type("Sprint")

    @property
    def race(self) -> Session:
        return self._by_type("Race")

    def _by_type(self, name: str) -> Session:
        value = next(
            (
                s
                for s in self.sessions
                if s.sub_type is not None and s.sub_type.value == name
            ),
            None,
        )
        if value is None:
            raise ValueError(f"No {name} found for {self.name}")
        return value

    @property
    def weekend_start_datetime(self) -> datetime:
        return self.sessions[0].start_date

    @property
    def weekend_end_datetime(self) -> datetime:
        return self.sessions[-1].end_date

    @property
    def folder_name(self) -> str:
        path = next((s.path for s in self.sessions if s.path is not None), None)
        if path is None:
            raise ValueError("No sessions available")
        return path.split("/")[1]

    @override
    def __str__(self) -> str:
        res = f"{self.name} ({self.weekend_start_datetime} - {self.weekend_end_datetime})\n\t"
        session_str = ", ".join([str(s.sub_type) for s in self.sessions])
        res += f"[{session_str}]"
        return res
