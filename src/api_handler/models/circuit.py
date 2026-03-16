from pydantic import BaseModel
from pydantic.fields import Field

class Circuit(BaseModel):
    key: int = Field(alias="Key")
    short_name: str = Field(alias="ShortName")