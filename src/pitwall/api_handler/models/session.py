from __future__ import annotations

from datetime import datetime, timedelta
from enum import StrEnum
from typing import Annotated, ClassVar, cast, override

from pydantic import (
    BeforeValidator,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)
from pydantic.alias_generators import to_pascal

from pitwall.api_handler.models.base import F1Model
from pitwall.api_handler.models.meeting_data import MeetingData


def _normalize_session_name(v: object) -> object:
    if not isinstance(v, str):
        return v
    key = v.strip().casefold()
    aliased = _SESSION_ALIASES.get(key, key)
    for member in SessionSubType:
        if member.value.casefold() == aliased:
            return member.value
    return v.strip()


class SessionType(StrEnum):
    PRACTICE = "Practice"
    QUALIFYING = "Qualifying"
    RACE = "Race"


_SESSION_ALIASES: dict[str, str] = {
    "sprint qualifying": "sprint shootout",
    "day 1": "practice 1",
    "day 2": "practice 2",
    "day 3": "practice 3",
}


class SessionSubType(StrEnum):
    PRACTICE_1 = "Practice 1"
    PRACTICE_2 = "Practice 2"
    PRACTICE_3 = "Practice 3"
    QUALIFYING = "Qualifying"
    SPRINT_SHOOTOUT = "Sprint Shootout"
    SPRINT = "Sprint"
    RACE = "Race"

    @override
    def __str__(self) -> str:
        return self.value

    @classmethod
    def parse(cls, value: str) -> SessionSubType:
        cleaned = _SESSION_ALIASES.get(value.strip().casefold(), value.strip())
        lookup = {m.value.casefold(): m for m in cls}
        result = lookup.get(cleaned.casefold())
        if result is None:
            valid = ", ".join(m.value for m in cls)
            raise ValueError(
                f"Unknown session type: {value!r}. Valid options: {valid}"
            ) from None
        return result


SessionSubTypeField = Annotated[
    SessionSubType, BeforeValidator(_normalize_session_name)
]


# TODO: eventually move status to be a StrEnum to control vocab bette
class ArchiveStatus(F1Model):
    status: str = Field(alias="Status")


# This is used to model SessionInfo.json information, which is more descriptive of a session than Meeting-level session info (class Session)
class SessionInfo(F1Model):
    """Rich session info from SessionInfo.json. Composes Session + Meeting context."""

    session: Session
    meeting: MeetingData = Field(alias="Meeting")
    archive_status: ArchiveStatus = Field(alias="ArchiveStatus")

    @model_validator(mode="before")
    @classmethod
    def _extract_session(cls, data: dict[str, object]) -> dict[str, object]:
        """Session fields live at the root level alongside Meeting/ArchiveStatus.
        Extract them into a nested 'session' key so Pydantic validates Session separately."""
        session_keys = {
            "Key",
            "Type",
            "Number",
            "Name",
            "StartDate",
            "EndDate",
            "GmtOffset",
            "Path",
        }
        session_data = {k: v for k, v in data.items() if k in session_keys}
        data["session"] = session_data
        return data


# This is used within Meeting to capture Meeting-level session info
class Session(F1Model):
    model_config: ClassVar[ConfigDict] = ConfigDict(
        populate_by_name=True, alias_generator=to_pascal
    )

    key: int
    type: SessionType | None
    number: int | None = None
    sub_type: SessionSubTypeField | None = Field(alias="Name", default=None)
    start_date: datetime
    end_date: datetime
    gmt_offset: timedelta
    path: str | None = None

    @field_validator("gmt_offset", mode="before")
    @classmethod
    def parse_gmt_offset(cls, v: str) -> timedelta:
        negative = v.startswith("-")
        v = v.lstrip("-")
        h, m, s = map(int, v.split(":"))
        td = timedelta(hours=h, minutes=m, seconds=s)
        return -td if negative else td

    @property
    def folder_name(self) -> str:
        if self.path is None:
            raise ValueError("Session has no path")
        return self.path.split("/")[2]


class FeedName(StrEnum):
    SESSION_INFO = "SessionInfo"
    ARCHIVE_STATUS = "ArchiveStatus"
    SESSION_DATA = "SessionData"
    SESSION_STATUS = "SessionStatus"
    CONTENT_STREAMS = "ContentStreams"
    AUDIO_STREAMS = "AudioStreams"
    HEARTBEAT = "Heartbeat"
    EXTRAPOLATED_CLOCK = "ExtrapolatedClock"

    TIMING_DATA = "TimingData"
    TIMING_DATA_F1 = "TimingDataF1"
    TIMING_APP_DATA = "TimingAppData"
    TIMING_STATS = "TimingStats"
    LAP_SERIES = "LapSeries"
    LAP_COUNT = "LapCount"
    TOP_THREE = "TopThree"
    TYRE_STINT_SERIES = "TyreStintSeries"
    CURRENT_TYRES = "CurrentTyres"

    CAR_DATA = "CarData.z"
    POSITION = "Position.z"

    RACE_CONTROL_MESSAGES = "RaceControlMessages"
    TLA_RCM = "TlaRcm"
    TRACK_STATUS = "TrackStatus"
    TEAM_RADIO = "TeamRadio"

    WEATHER_DATA = "WeatherData"
    WEATHER_DATA_SERIES = "WeatherDataSeries"

    DRIVER_LIST = "DriverList"
    PIT_LANE_TIME_COLLECTION = "PitLaneTimeCollection"

    CHAMPIONSHIP_PREDICTION = "ChampionshipPrediction"
    DRIVER_SCORE = "DriverScore"
    DRIVER_RACE_INFO = "DriverRaceInfo"


class Feed(F1Model):
    key_frame_path: str
    stream_path: str

    @property
    def compressed(self) -> bool:
        return ".z." in self.key_frame_path


class FeedNotAvailableError(Exception):
    def __init__(self, name: str) -> None:
        super().__init__(f"Feed '{name}' is not available for this session")


_FEED_MAP: dict[str, str] = {
    "SessionInfo": "session_info",
    "ArchiveStatus": "archive_status",
    "SessionData": "session_data",
    "SessionStatus": "session_status",
    "ContentStreams": "content_streams",
    "AudioStreams": "audio_streams",
    "Heartbeat": "heartbeat",
    "ExtrapolatedClock": "extrapolated_clock",
    "TimingData": "timing_data",
    "TimingDataF1": "timing_data_f1",
    "TimingAppData": "timing_app_data",
    "TimingStats": "timing_stats",
    "LapSeries": "lap_series",
    "LapCount": "lap_count",
    "TopThree": "top_three",
    "TyreStintSeries": "tyre_stint_series",
    "CurrentTyres": "current_tyres",
    "CarData.z": "car_data",
    "Position.z": "position",
    "RaceControlMessages": "race_control_messages",
    "TlaRcm": "tla_rcm",
    "TrackStatus": "track_status",
    "TeamRadio": "team_radio",
    "WeatherData": "weather_data",
    "WeatherDataSeries": "weather_data_series",
    "DriverList": "driver_list",
    "PitLaneTimeCollection": "pit_lane_time_collection",
    "ChampionshipPrediction": "championship_prediction",
    "DriverScore": "driver_score",
    "DriverRaceInfo": "driver_race_info",
}

_SENTINEL = object()


class SessionIndex(F1Model):
    model_config: ClassVar[ConfigDict] = ConfigDict(arbitrary_types_allowed=True)

    # Metadata
    session_info: Feed | object = _SENTINEL
    archive_status: Feed | object = _SENTINEL
    session_data: Feed | object = _SENTINEL
    session_status: Feed | object = _SENTINEL
    content_streams: Feed | object = _SENTINEL
    audio_streams: Feed | object = _SENTINEL
    heartbeat: Feed | object = _SENTINEL
    extrapolated_clock: Feed | object = _SENTINEL

    # Timing & Laps
    timing_data: Feed | object = _SENTINEL
    timing_data_f1: Feed | object = _SENTINEL
    timing_app_data: Feed | object = _SENTINEL
    timing_stats: Feed | object = _SENTINEL
    lap_series: Feed | object = _SENTINEL
    lap_count: Feed | object = _SENTINEL
    top_three: Feed | object = _SENTINEL
    tyre_stint_series: Feed | object = _SENTINEL
    current_tyres: Feed | object = _SENTINEL

    # Telemetry
    car_data: Feed | object = _SENTINEL
    position: Feed | object = _SENTINEL

    # Race Control
    race_control_messages: Feed | object = _SENTINEL
    tla_rcm: Feed | object = _SENTINEL
    track_status: Feed | object = _SENTINEL
    team_radio: Feed | object = _SENTINEL

    # Weather
    weather_data: Feed | object = _SENTINEL
    weather_data_series: Feed | object = _SENTINEL

    # Drivers & Pit
    driver_list: Feed | object = _SENTINEL
    pit_lane_time_collection: Feed | object = _SENTINEL

    # Championship
    championship_prediction: Feed | object = _SENTINEL
    driver_score: Feed | object = _SENTINEL
    driver_race_info: Feed | object = _SENTINEL

    # Overflow
    extra: dict[str, Feed] = Field(default_factory=dict)

    def __getattr__(self, name: str) -> Feed:
        value: object = super().__getattribute__(name)  # pyright: ignore[reportAny]
        if value is _SENTINEL:
            raise FeedNotAvailableError(name)
        if not isinstance(value, Feed):
            raise AttributeError(name)
        return value

    @model_validator(mode="before")
    @classmethod
    def _parse_index(
        cls, data: dict[str, dict[str, str]]
    ) -> dict[str, Feed | dict[str, Feed]]:
        feeds_raw = cast(
            dict[str, dict[str, str]],
            data.get("Feeds", data),
        )

        feed_kwargs: dict[str, Feed | dict[str, Feed]] = {}
        extra: dict[str, Feed] = {}

        for api_name, paths in feeds_raw.items():
            feed = Feed(
                key_frame_path=paths["KeyFramePath"],
                stream_path=paths["StreamPath"],
            )
            field_name = _FEED_MAP.get(api_name)
            if field_name:
                feed_kwargs[field_name] = feed
            else:
                extra[api_name] = feed

        feed_kwargs["extra"] = extra
        return feed_kwargs

    @override
    def __str__(self) -> str:
        set_feeds = {k: v for k, v in self.__dict__.items() if isinstance(v, Feed)}
        extra = self.__dict__.get("extra", {})
        if extra:
            set_feeds["extra"] = extra
        return f"SessionFeeds({set_feeds})"
