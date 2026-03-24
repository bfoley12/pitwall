import warnings

import httpx
from pydantic import BaseModel
from pydantic.functional_validators import field_validator

from pitwall.api_handler.models.meeting import Meeting
from pitwall.api_handler.models.season import Season
from pitwall.api_handler.models.session import SessionSubType


class PathResolver(BaseModel):
    _scheme: str = "https"
    _domain: str = "livetiming.formula1.com/static"
    _slug: str = "https://livetiming.formula1.com/static"
    year: int | None = None
    meeting: str | None = None
    session: str | None = None
    file: str | None = None

    @field_validator("year", mode="before")
    @classmethod
    def validate_year(cls, value: int) -> int:
        if value < 2018 or value > 2026:
            raise ValueError(
                f"year must be between 2018 and 2026 (inclusive). Gave: {value}"
            )
        return value

    # TODO: Validate meetings via list of meetings from given year
    # Need to model the year/Index.json response
    # @field_validator("meeting", mode="after")

    def with_year(self, year: int) -> "PathResolver":
        self.year = year
        return self

    def with_meeting(self, meeting: str) -> "PathResolver":
        self.meeting = meeting
        return self

    def with_session(self, session: SessionSubType) -> "PathResolver":
        self.session = session
        return self

    def with_file(self, file: str) -> "PathResolver":
        self.file = file
        return self

    def get_meeting(self, meeting: str) -> Meeting:
        if self.year is None:
            raise ValueError("year required to get meeting")
        response = httpx.get("/".join([self._slug, str(self.year), "Index.json"]))
        season = Season.model_validate(response.json())
        meeting_instance = season.keyframe.get_meeting(meeting)
        return meeting_instance

    @property
    def url(self) -> str:
        url = f"{self._scheme}://{self._domain}"
        if self.year:
            url += f"/{self.year}"
        if self.meeting:
            meeting = self.get_meeting(self.meeting)
            url += "/" + meeting.folder_name
            if self.session:
                s = self.session.replace(" ", "_").lower()
                if s in ["practice_1", "fp1", "free_practice_1"]:
                    url += "/" + meeting.fp1.folder_name
                elif s in ["practice_2", "fp2", "free_practice_2"]:
                    url += "/" + meeting.fp2.folder_name
                elif s in ["practice_3", "fp3", "free_practice_3"]:
                    url += "/" + meeting.fp3.folder_name
                elif s in ["q", "qualifying"]:
                    url += "/" + meeting.q.folder_name
                elif s in ["sq", "sprint_qualifying", "sprint_shootout"]:
                    url += "/" + meeting.sq.folder_name
                elif s in ["sprint", "sprint_race", "sr"]:
                    url += "/" + meeting.sprint.folder_name
                elif s in ["r", "race"]:
                    url += "/" + meeting.race.folder_name
        if self.meeting is None and self.session is not None:
            warnings.warn(
                "Attempted to build url with session, without specifying meeting",
                stacklevel=2,
            )
        if self.file:
            url += "/" + self.file
        if self.file is None:
            url += "/" + "Index.json"
        return url
