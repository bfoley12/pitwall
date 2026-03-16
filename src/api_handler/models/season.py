from typing import override

from pydantic import Field, model_validator

from src.api_handler.models.base import F1Model

from .meeting import Meeting


class Season(F1Model):
    year: int = Field(alias="Year")
    meetings: list[Meeting] = Field(alias="Meetings")

    @model_validator(mode="after")
    def sort_meetings(self) -> "Season":
        self.meetings.sort(key=lambda m: m.number)
        return self

    def get_meeting(self, location: str) -> Meeting:
        location = location.replace(" ", "_").lower()
        result = next(
            (
                m
                for m in self.meetings
                if m.location.replace(" ", "_").lower() == location
            ),
            None,
        )
        if result is None:
            raise ValueError(f"No meeting found for location: {location}")
        return result

    @override
    def __str__(self) -> str:
        res = f"Year: {self.year}\n"
        meetings_str = "\n".join(str(meeting) for meeting in self.meetings)
        res += f"{meetings_str}"
        return res
