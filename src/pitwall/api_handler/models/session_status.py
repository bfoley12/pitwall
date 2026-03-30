from typing import ClassVar
from pitwall.api_handler.models.base import F1DataContainer, F1Frame, F1Stream
import polars as pl

class SessionStatusKeyframe(F1Frame):
    status: str
    started: str | None = None


class SessionStatusStream(F1Stream):
    SCHEMA: ClassVar[dict[str, pl.DataType]] = {
        "timestamp": pl.Duration("ms"),
        "status": pl.String(),
        "started": pl.String(),
    }


class SessionStatus(F1DataContainer[SessionStatusKeyframe, SessionStatusStream]):
    KEYFRAME_FILE: ClassVar[str | None] = "SessionStatus.json"
    STREAM_FILE: ClassVar[str | None] = "SessionStatus.jsonStream"

    keyframe: SessionStatusKeyframe
    stream: SessionStatusStream
