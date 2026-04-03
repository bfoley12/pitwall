from typing import ClassVar

import polars as pl

from pitwall.api_handler.models.base import F1DataContainer, F1Frame, F1Stream
from pitwall.api_handler.registry import register


class SessionStatusKeyframe(F1Frame):
    status: str
    started: str | None = None


class SessionStatusStream(F1Stream):
    SCHEMA: ClassVar[dict[str, pl.DataType]] = {
        "timestamp": pl.Duration("ms"),
        "status": pl.String(),
        "started": pl.String(),
    }


@register
class SessionStatus(F1DataContainer[SessionStatusKeyframe, SessionStatusStream]):
    KEYFRAME_FILE: ClassVar[str | None] = "SessionStatus.json"
    STREAM_FILE: ClassVar[str | None] = "SessionStatus.jsonStream"

    keyframe: SessionStatusKeyframe
    stream: SessionStatusStream
