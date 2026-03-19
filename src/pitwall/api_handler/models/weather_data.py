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
