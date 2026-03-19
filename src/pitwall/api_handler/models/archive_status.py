from typing import ClassVar
from pydantic import ConfigDict
from pydantic.alias_generators import to_pascal
from .base import F1Model


# TODO: eventually move status to be a StrEnum to control vocab bette
class ArchiveStatus(F1Model):
    status: str
