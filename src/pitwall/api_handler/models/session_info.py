from typing import Any, ClassVar

from pydantic import model_validator

from .archive_status import ArchiveStatusFrame
from .base import F1DataContainer, F1Model
from .meeting_data import MeetingData
from .session import Session


class SessionInfoKeyframe(F1Model):
    """Rich session info — composes Session + Meeting context."""

    session: Session
    meeting: MeetingData
    archive_status: ArchiveStatusFrame

    @model_validator(mode="before")
    @classmethod
    def _extract_session(cls, data: Any) -> Any:
        if not isinstance(data, dict) or "session" in data:
            return data

        session_keys = {
            "Key",
            "Type",
            "Number",
            "Name",
            "StartDate",
            "EndDate",
            "GmtOffset",
            "Path",
        }
        session_data = {k: v for k, v in data.items() if k in session_keys}
        data["session"] = session_data
        return data


class SessionInfo(F1DataContainer):
    """Session metadata from SessionInfo.json.

    keyframe.session: Session timing, type, path.
    keyframe.meeting: Meeting name, location, circuit, country.
    keyframe.archive_status: Whether data archival is complete.
    """

    KEYFRAME_FILE: ClassVar[str | None] = "SessionInfo.json"

    keyframe: SessionInfoKeyframe | None = None
