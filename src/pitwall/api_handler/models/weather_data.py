from typing import ClassVar

import polars as pl

from pitwall.api_handler.models.base import F1DataContainer, F1Frame, F1Stream


# TODO: Add units
class WeatherDataKeyframe(F1Frame):
    air_temp: float
    humidity: float
    pressure: float
    rainfall: float
    track_temp: float
    wind_direction: int
    wind_speed: float


class WeatherDataStream(F1Stream):
    SCHEMA: ClassVar[dict[str, pl.DataType]] = {
        "air_temp": pl.Float16(),
        "humidity": pl.Float16(),
        "pressure": pl.Float16(),
        # TODO: Determine if rainfall is float or int
        "rainfall": pl.Float16(),
        "track_temp": pl.Float16(),
        "wind_direction": pl.Int16(),
        "wind_speed": pl.Float16(),
    }


class WeatherData(F1DataContainer):
    KEYFRAME_FILE: ClassVar[str | None] = "WeatherData.json"
    STREAM_FILE: ClassVar[str | None] = "WeatherData.jsonStream"

    keyframe: WeatherDataKeyframe
    stream: WeatherDataStream
