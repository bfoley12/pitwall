from typing import ClassVar

import polars as pl

from .base import F1DataContainer, F1Frame, F1Stream


# TODO: eventually move status to be a StrEnum to control vocab better
class ArchiveStatusStream(F1Stream):
    SCHEMA: ClassVar[dict[str, pl.DataType]] = {"status": pl.String()}


class ArchiveStatusFrame(F1Frame):
    status: str


class ArchiveStatus(F1DataContainer):
    KEYFRAME_FILE: ClassVar[str | None] = "ArchiveStatus.json"
    STREAM_FILE: ClassVar[str | None] = "ArchiveStatus.jsonStream"

    frame: ArchiveStatusFrame
    stream: ArchiveStatusStream
