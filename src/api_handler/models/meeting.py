from typing import override
from pydantic import BaseModel, Field
from datetime import datetime
from .country import Country
from .circuit import Circuit
from .session import Session

SESSION_KEYS = {
    "Practice 1": "FP1",
    "Practice 2": "FP2",
    "Practice 3": "FP3",
    "Qualifying": "Q",
    "Sprint Shootout": "SQ",
    "Race": "Race"
}


class Meeting(BaseModel):
    key: int = Field(alias="Key")
    code: str = Field(alias="Code")
    number: int = Field(alias="Number")
    location: str = Field(alias="Location")
    official_name: str = Field(alias="OfficialName")
    name: str = Field(alias="Name")
    country: Country = Field(alias="Country")
    circuit: Circuit = Field(alias="Circuit")
    sessions: list[Session] = Field(alias="Sessions")
    
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
        value = next((s for s in self.sessions if s.sub_type is not None and s.sub_type.value == name), None)
        if value is None:
            raise ValueError(f"No {name} found for {self.name}")
        return value
    
    @property
    def weekend_start_datetime(self) -> datetime:
        return self.fp1.start_date
        
    # TODO: sort sessions by time and select the last one to avoid looking for a race during pre-season testing
    @property
    def weekend_end_datetime(self) -> datetime:
        return self.race.end_date
        
    @property
    def folder_name(self) -> str:
        path = next((s.path for s in self.sessions if s.path is not None), None)
        if path is None:
            raise ValueError("No sessions available")
        return path.split("/")[1]
        
    @override
    def __str__(self):
        res = f"{self.name} ({self.weekend_start_datetime} - {self.weekend_end_datetime})\n\t"
        session_str = ", ".join([str(s.sub_type) for s in self.sessions])
        res += f"[{session_str}]"
        return res