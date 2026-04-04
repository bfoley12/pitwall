from datetime import datetime
from typing import ClassVar, cast, override

import polars as pl
from pydantic import JsonValue

from pitwall.api_handler.models.base import (
    F1DataContainer,
    F1Frame,
    F1Model,
    F1Stream,
    ParsedValue,
)
from pitwall.api_handler.registry import register


# Technically identical to WeatherDataKeyframe, but kept distinct incase the schemas evolve separately
class WeatherDataInfo(F1Model):
    air_temp: float
    humidity: float
    pressure: float
    rainfall: float
    track_temp: float
    wind_direction: int
    wind_speed: float


class WeatherDataEntry(F1Model):
    timestamp: datetime
    weather: WeatherDataInfo


class WeatherDataSeriesKeyframe(F1Frame):
    series: list[WeatherDataEntry]


class WeatherDataSeriesStream(F1Stream):
    SCHEMA: ClassVar[dict[str, pl.DataType]] = {
        "timestamp": pl.Duration("ms"),
        "utc": pl.Datetime(),
        "air_temp": pl.Float16(),
        "humidity": pl.Float16(),
        "pressure": pl.Float16(),
        "rainfall": pl.Float16(),
        "track_temp": pl.Float16(),
        "wind_direction": pl.UInt16(),
        "wind_speed": pl.Float16(),
    }

    @override
    @classmethod
    def _extract_rows(
        cls, timestamp_ms: int, data: dict[str, JsonValue]
    ) -> list[dict[str, ParsedValue]]:
        rows: list[dict[str, ParsedValue]] = []
        series = cls._as_dict(data.get("Series"))

        for entry in series.values():
            if entry is None:
                continue
            if isinstance(entry, list):
                entry = entry[0]
            entry = cast(dict[str, JsonValue], entry)
            weather = cast(dict[str, JsonValue], entry.get("Weather", {}))
            utc = entry.get("Timestamp")
            rows.append(
                {
                    "timestamp": timestamp_ms,
                    "utc": cls._parse_utc(utc) if isinstance(utc, str) else utc,
                    "air_temp": weather.get("AirTemp"),
                    "humidity": weather.get("Humidity"),
                    "pressure": weather.get("Pressure"),
                    "rainfall": weather.get("Rainfall"),
                    "track_temp": weather.get("TrackTemp"),
                    "wind_direction": weather.get("WindDirection"),
                    "wind_speed": weather.get("WindSpeed"),
                }
            )

        return rows


@register
class WeatherDataSeries(
    F1DataContainer[WeatherDataSeriesKeyframe, WeatherDataSeriesStream]
):
    KEYFRAME_FILE: ClassVar[str | None] = "WeatherDataSeries.json"
    STREAM_FILE: ClassVar[str | None] = "WeatherDataSeries.jsonStream"

    keyframe: WeatherDataSeriesKeyframe
    stream: WeatherDataSeriesStream
