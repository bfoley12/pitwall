from typing import ClassVar

from pydantic import ConfigDict
from pydantic.alias_generators import to_pascal

from pitwall.api_handler.models.base import F1Model


# TODO: Add units
class WeatherData(F1Model):
    air_temp: float
    humidity: float
    pressure: float
    rainfall: float
    track_temp: float
    wind_direction: int
    wind_speed: float
