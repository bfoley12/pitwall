from pydantic import Field, model_validator

from pitwall.api_handler.models.base import F1Model


class DriverInfo(F1Model):
    racing_number: int = Field(alias="RacingNumber")
    broadcast_name: str = Field(alias="BroadcastName")
    full_name: str = Field(alias="FullName")
    tla: str = Field(alias="Tla")
    line: int = Field(alias="Line")
    team_name: str = Field(alias="TeamName")
    team_colour: str = Field(alias="TeamColour")
    first_name: str = Field(alias="FirstName")
    last_name: str = Field(alias="LastName")
    reference: str = Field(alias="Reference")
    headshot_url: str | None = Field(alias="HeadshotUrl", default=None)

    # Newer codes
    public_id_right: str | None = Field(alias="PublicIdRight", default=None)

    # Older codes
    country_code: str | None = Field(alias="CountryCode", default=None)

    @property
    def team_color(self) -> str:
        return self.team_colour


class DriverList(F1Model):
    drivers: dict[str, DriverInfo] = Field(alias="root")

    @model_validator(mode="before")
    @classmethod
    def _wrap(cls, data: dict[str, object]) -> dict[str, object]:
        return {"root": data}
