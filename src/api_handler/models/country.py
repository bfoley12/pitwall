from pydantic import BaseModel, Field

class Country(BaseModel):
    key: int = Field(alias="Key")
    code: str = Field(alias="Code")
    name: str = Field(alias="Name")