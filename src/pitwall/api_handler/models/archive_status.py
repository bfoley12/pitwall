from typing import ClassVar

import polars as pl

from pitwall.api_handler.registry import register

from .base import F1DataContainer, F1Frame, F1Stream


# TODO: eventually move status to be a StrEnum to control vocab better
class ArchiveStatusStream(F1Stream):
    SCHEMA: ClassVar[dict[str, pl.DataType]] = {"status": pl.String()}


class ArchiveStatusFrame(F1Frame):
    status: str


@register
class ArchiveStatus(F1DataContainer[ArchiveStatusFrame, ArchiveStatusStream]):
    KEYFRAME_FILE: ClassVar[str | None] = "ArchiveStatus.json"
    STREAM_FILE: ClassVar[str | None] = "ArchiveStatus.jsonStream"

    keyframe: ArchiveStatusFrame
    stream: ArchiveStatusStream
