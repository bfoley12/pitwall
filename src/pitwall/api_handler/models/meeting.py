from datetime import datetime
from typing import override

from pydantic import model_validator

from .base import F1Model
from .circuit import Circuit
from .country import Country
from .meeting_data import MeetingData
from .session import Session

SESSION_KEYS = {
    "Practice 1": "FP1",
    "Practice 2": "FP2",
    "Practice 3": "FP3",
    "Qualifying": "Q",
    "Sprint Shootout": "SQ",
    "Race": "Race",
}


# TODO: Rework how meetings are loaded from F1's API
# - Currently, we need to get the Season, then query the Season object for keyframe.get_meeting(name)
# - We should probably allow F1Client.get(model=Meeting, year=year, meeting=meeting) and meeting knows the load the year?
class Meeting(F1Model):
    code: str
    number: int
    data: MeetingData
    sessions: list[Session]

    @model_validator(mode="before")
    @classmethod
    def _extract_data(cls, data: dict[str, object]) -> dict[str, object]:
        session_keys = {"Sessions"}
        meeting_data = {k: v for k, v in data.items() if k not in session_keys}
        data["data"] = meeting_data
        return data

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

    @property
    def key(self) -> int:
        return self.data.key

    @property
    def location(self) -> str:
        return self.data.location

    @property
    def official_name(self) -> str:
        return self.data.official_name

    @property
    def name(self) -> str:
        return self.data.name

    @property
    def country(self) -> Country:
        return self.data.country

    @property
    def circuit(self) -> Circuit:
        return self.data.circuit

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
            raise ValueError(f"No {name} found for {self.data.name}")
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
        res = f"{self.data.name} ({self.weekend_start_datetime} - {self.weekend_end_datetime})\n\t"
        session_str = ", ".join([str(s.sub_type) for s in self.sessions])
        res += f"[{session_str}]"
        return res
