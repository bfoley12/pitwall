from typing import ClassVar

import polars as pl

from pitwall.api_handler.models.base import F1DataContainer, F1Frame, F1Stream
from pitwall.api_handler.registry import register


class TrackStatusStream(F1Stream):
    SCHEMA: ClassVar[dict[str, pl.DataType]] = {
        "timestamp": pl.Duration("ms"),
        # Make status/message an enum to help specify vocabulary?
        "status": pl.UInt8(),
        "message": pl.String(),
    }


class TrackStatusKeyframe(F1Frame):
    status: int
    message: str


@register
class TrackStatus(F1DataContainer[TrackStatusKeyframe, TrackStatusStream]):
    KEYFRAME_FILE: ClassVar[str | None] = "TrackStatus.json"
    STREAM_FILE: ClassVar[str | None] = "TrackStatus.jsonStream"

    keyframe: TrackStatusKeyframe
    stream: TrackStatusStream
