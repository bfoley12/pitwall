from pydantic.fields import Field

from pitwall.api_handler.models.base import F1Model


class Circuit(F1Model):
    key: int = Field(alias="Key")
    short_name: str = Field(alias="ShortName")
