from datetime import datetime
from enum import StrEnum

from .base import F1Model


class StreamType(StrEnum):
    COMMENTARY = "Commentary"
    AUDIO = "Audio"


class ContentStream(F1Model):
    type: StreamType
    name: str
    language: str
    uri: str
    path: str | None = None
    utc: datetime
