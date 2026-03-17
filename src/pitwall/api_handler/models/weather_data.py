from pydantic import Field

from pitwall.api_handler.models.base import F1Model


# TODO: Add units
class WeatherData(F1Model):
    air_temp: float = Field(alias="AirTemp")
    humidity: float = Field(alias="Humidity")
    pressure: float = Field(alias="Pressure")
    rainfall: float = Field(alias="Rainfall")
    track_temp: float = Field(alias="TrackTemp")
    wind_direction: int = Field(alias="WindDirection")
    wind_speed: float = Field(alias="WindSpeed")
