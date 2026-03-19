from typing import ClassVar

from pydantic import ConfigDict, Field, model_validator
from pydantic.alias_generators import to_pascal

from pitwall.api_handler.models.base import F1Model


class DriverInfo(F1Model):
    model_config: ClassVar[ConfigDict] = ConfigDict(
        populate_by_name=True, alias_generator=to_pascal
    )
    racing_number: int
    broadcast_name: str
    full_name: str
    tla: str
    line: int
    team_name: str
    team_colour: str
    first_name: str
    last_name: str
    reference: str
    headshot_url: str | None = Field(default=None)

    # Newer codes
    public_id_right: str | None = Field(default=None)

    # Older codes
    country_code: str | None = Field(default=None)

    @property
    def team_color(self) -> str:
        return self.team_colour


class DriverList(F1Model):
    drivers: dict[str, DriverInfo]

    @model_validator(mode="before")
    @classmethod
    def _wrap(cls, data: dict[str, object]) -> dict[str, object]:
        return {"drivers": data}
