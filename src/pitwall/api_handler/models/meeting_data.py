from pitwall.api_handler.models.base import F1Model
from pitwall.api_handler.models.circuit import Circuit
from pitwall.api_handler.models.country import Country


# Had to make this a separate file to avoid circular imports of Session <=> Meeting
# TODO: Make a dataclass with slots? Probably overoptimization
class MeetingData(F1Model):
    key: int
    location: str
    official_name: str
    name: str
    country: Country
    circuit: Circuit
