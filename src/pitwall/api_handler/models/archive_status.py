from .base import F1Model


# TODO: eventually move status to be a StrEnum to control vocab bette
class ArchiveStatus(F1Model):
    status: str
