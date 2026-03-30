from typing import ClassVar, override

import polars as pl
from pydantic import JsonValue, model_validator

from pitwall.api_handler.registry import register

from .archive_status import ArchiveStatusFrame
from .base import F1DataContainer, F1Frame, F1Stream, ParsedValue
from .meeting_data import MeetingData
from .session import Session


class SessionInfoKeyframe(F1Frame):
    """Rich session info — composes Session + Meeting context."""

    session: Session
    meeting: MeetingData
    archive_status: ArchiveStatusFrame

    @model_validator(mode="before")
    @classmethod
    def _extract_session(cls, data: dict[str, JsonValue]) -> dict[str, JsonValue]:
        if "session" in data:
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
        data["session"] = {k: v for k, v in data.items() if k in session_keys}
        return data


class SessionInfoStream(F1Stream):
    SCHEMA: ClassVar[dict[str, pl.DataType]] = {
        "timestamp": pl.Duration("ms"),
        "meeting_key": pl.UInt16(),
        "meeting_name": pl.Utf8(),
        "meeting_official_name": pl.Utf8(),
        "meeting_location": pl.Utf8(),
        "meeting_number": pl.UInt8(),
        "country_key": pl.UInt16(),
        "country_code": pl.Utf8(),
        "country_name": pl.Utf8(),
        "circuit_key": pl.UInt16(),
        "circuit_short_name": pl.Utf8(),
        "session_status": pl.Utf8(),
        "archive_status": pl.Utf8(),
        "session_key": pl.UInt16(),
        "session_type": pl.Utf8(),
        "session_number": pl.UInt8(),
        "session_name": pl.Utf8(),
        "start_date": pl.Utf8(),
        "end_date": pl.Utf8(),
        "gmt_offset": pl.Utf8(),
        "path": pl.Utf8(),
    }

    @override
    @classmethod
    def _extract_rows(
        cls, timestamp_ms: int, data: dict[str, JsonValue]
    ) -> list[dict[str, ParsedValue]]:
        meeting = cls._as_dict(data.get("Meeting"))
        country = cls._as_dict(meeting.get("Country"))
        circuit = cls._as_dict(meeting.get("Circuit"))
        archive = cls._as_dict(data.get("ArchiveStatus"))

        return [
            {
                "timestamp": timestamp_ms,
                "meeting_key": meeting.get("Key"),
                "meeting_name": meeting.get("Name"),
                "meeting_official_name": meeting.get("OfficialName"),
                "meeting_location": meeting.get("Location"),
                "meeting_number": meeting.get("Number"),
                "country_key": country.get("Key"),
                "country_code": country.get("Code"),
                "country_name": country.get("Name"),
                "circuit_key": circuit.get("Key"),
                "circuit_short_name": circuit.get("ShortName"),
                "session_status": data.get("SessionStatus"),
                "archive_status": archive.get("Status"),
                "session_key": data.get("Key"),
                "session_type": data.get("Type"),
                "session_number": data.get("Number"),
                "session_name": data.get("Name"),
                "start_date": data.get("StartDate"),
                "end_date": data.get("EndDate"),
                "gmt_offset": data.get("GmtOffset"),
                "path": data.get("Path"),
            }
        ]


@register
class SessionInfo(F1DataContainer[SessionInfoKeyframe, SessionInfoStream]):
    """Session metadata from SessionInfo.json.

    keyframe.session: Session timing, type, path.
    keyframe.meeting: Meeting name, location, circuit, country.
    keyframe.archive_status: Whether data archival is complete.
    """

    KEYFRAME_FILE: ClassVar[str | None] = "SessionInfo.json"
    STREAM_FILE: ClassVar[str | None] = "SessionInfo.jsonStream"

    keyframe: SessionInfoKeyframe
    stream: SessionInfoStream
