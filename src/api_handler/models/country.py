from pydantic import Field

from src.api_handler.models.base import F1Model


class Country(F1Model):
    key: int = Field(alias="Key")
    code: str = Field(alias="Code")
    name: str = Field(alias="Name")
