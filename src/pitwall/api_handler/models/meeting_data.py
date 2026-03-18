from pydantic import Field

from pitwall.api_handler.models.base import F1Model
from pitwall.api_handler.models.circuit import Circuit
from pitwall.api_handler.models.country import Country

# Had to make this a separate file to avoid circular imports of Session <=> Meeting


# Moved out of Meeting to allow for reuse within SessionInfo
class MeetingData(F1Model):
    key: int = Field(alias="Key")
    location: str = Field(alias="Location")
    official_name: str = Field(alias="OfficialName")
    name: str = Field(alias="Name")
    country: Country = Field(alias="Country")
    circuit: Circuit = Field(alias="Circuit")
