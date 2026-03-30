from dataclasses import dataclass

import typer

from pitwall.api_handler.client import DirectClient
from pitwall.api_handler.models.archive_status import ArchiveStatus
from pitwall.api_handler.models.car_data import CarData
from pitwall.api_handler.models.championship_prediction import ChampionshipPrediction
from pitwall.api_handler.models.content_streams import ContentStreams
from pitwall.api_handler.models.current_tyres import CurrentTyres
from pitwall.api_handler.models.driver_list import DriverList
from pitwall.api_handler.models.driver_race_info import DriverRaceInfo
from pitwall.api_handler.models.driver_tracker import DriverTracker
from pitwall.api_handler.models.lap_count import LapCount
from pitwall.api_handler.models.lap_series import LapSeries
from pitwall.api_handler.models.pit_lane_time_collection import PitLaneTimeCollection
from pitwall.api_handler.models.pit_stop import PitStop
from pitwall.api_handler.models.pit_stop_series import PitStopSeries
from pitwall.api_handler.models.position import Position
from pitwall.api_handler.models.race_control_messages import RaceControlMessages
from pitwall.api_handler.models.season import Season
from pitwall.api_handler.models.session import SessionIndex, SessionSubType
from pitwall.api_handler.models.session_info import SessionInfo
from pitwall.api_handler.models.timing_app_data import TimingAppData
from pitwall.api_handler.models.timing_data import TimingDataF1
from pitwall.api_handler.models.timing_stats import TimingStats
from pitwall.api_handler.models.track_status import TrackStatus
from pitwall.api_handler.models.tyre_stint_series import TyreStintSeries
from pitwall.api_handler.models.weather_data import WeatherData

app = typer.Typer()


@dataclass(frozen=True)
class Defaults:
    year: int = 2023
    meeting: str = "Yas Marina"
    session: str = "Race"


DEFAULTS = Defaults()
client = DirectClient()


@app.command()
def season(year: int = DEFAULTS.year) -> None:
    print(client.get(model=Season, year=year))


@app.command()
def meeting(year: int = DEFAULTS.year, name: str = DEFAULTS.meeting) -> None:
    res = client.get(model=Season, year=year).keyframe.get_meeting(name)
    print(res.sessions[0])


@app.command()
def session_index(
    year: int = DEFAULTS.year,
    meeting: str = DEFAULTS.meeting,
    name: str = DEFAULTS.session,
) -> None:
    print(
        client.get(
            model=SessionIndex,
            year=year,
            meeting=meeting,
            session=SessionSubType.parse(name),
        )
    )


@app.command()
def session_info(
    year: int = DEFAULTS.year,
    meeting: str = DEFAULTS.meeting,
    name: str = DEFAULTS.session,
) -> None:
    print(
        client.get(
            model=SessionInfo,
            year=year,
            meeting=meeting,
            session=SessionSubType.parse(name),
        )
    )


@app.command()
def timing(
    year: int = DEFAULTS.year,
    meeting: str = DEFAULTS.meeting,
    session: str = DEFAULTS.session,
) -> None:
    print(
        client.get(
            model=TimingDataF1,
            year=year,
            meeting=meeting,
            session=SessionSubType.parse(session),
        )
    )


@app.command()
def timing_app(
    year: int = DEFAULTS.year,
    meeting: str = DEFAULTS.meeting,
    session: str = DEFAULTS.session,
) -> None:
    print(
        client.get(
            model=TimingAppData,
            year=year,
            meeting=meeting,
            session=SessionSubType.parse(session),
        )
    )


@app.command()
def timing_stats(
    year: int = DEFAULTS.year,
    meeting: str = DEFAULTS.meeting,
    session: str = DEFAULTS.session,
) -> None:
    print(
        client.get(
            model=TimingStats,
            year=year,
            meeting=meeting,
            session=SessionSubType.parse(session),
        )
    )


@app.command()
def car_data(
    year: int = DEFAULTS.year,
    meeting: str = DEFAULTS.meeting,
    session: str = DEFAULTS.session,
) -> None:
    df = client.get(
        model=CarData, year=year, meeting=meeting, session=SessionSubType.parse(session)
    )
    print(df)


@app.command()
def position_data(
    year: int = DEFAULTS.year,
    meeting: str = DEFAULTS.meeting,
    session: str = DEFAULTS.session,
) -> None:
    print(
        client.get(
            model=Position,
            year=year,
            meeting=meeting,
            session=SessionSubType.parse(session),
        )
    )


@app.command()
def weather_data(
    year: int = DEFAULTS.year,
    meeting: str = DEFAULTS.meeting,
    session: str = DEFAULTS.session,
) -> None:
    print(
        client.get(
            model=WeatherData,
            year=year,
            meeting=meeting,
            session=SessionSubType.parse(session),
        )
    )


@app.command()
def current_tyres(
    year: int = DEFAULTS.year,
    meeting: str = DEFAULTS.meeting,
    session: str = DEFAULTS.session,
) -> None:
    print(
        client.get(
            model=CurrentTyres,
            year=year,
            meeting=meeting,
            session=SessionSubType.parse(session),
        )
    )


@app.command()
def tyre_stints(
    year: int = DEFAULTS.year,
    meeting: str = DEFAULTS.meeting,
    session: str = DEFAULTS.session,
) -> None:
    print(
        client.get(
            model=TyreStintSeries,
            year=year,
            meeting=meeting,
            session=SessionSubType.parse(session),
        )
    )


@app.command()
def get_rcm(
    year: int = DEFAULTS.year,
    meeting: str = DEFAULTS.meeting,
    session: str = DEFAULTS.session,
) -> None:
    print(
        client.get(
            model=RaceControlMessages,
            year=year,
            meeting=meeting,
            session=SessionSubType.parse(session),
        )
    )


@app.command()
def track_status(
    year: int = DEFAULTS.year,
    meeting: str = DEFAULTS.meeting,
    session: str = DEFAULTS.session,
) -> None:
    df = client.get(
        model=TrackStatus,
        year=year,
        meeting=meeting,
        session=SessionSubType.parse(session),
    )
    print(df)


@app.command()
def pit_lane_time(
    year: int = DEFAULTS.year,
    meeting: str = DEFAULTS.meeting,
    session: str = DEFAULTS.session,
) -> None:
    df = client.get(
        model=PitLaneTimeCollection,
        year=year,
        meeting=meeting,
        session=SessionSubType.parse(session),
    )
    print(df)


@app.command()
def pit_stops(
    year: int = DEFAULTS.year,
    meeting: str = DEFAULTS.meeting,
    session: str = DEFAULTS.session,
) -> None:
    df = client.get(
        model=PitStop, year=year, meeting=meeting, session=SessionSubType.parse(session)
    )
    print(df)


@app.command()
def pit_stop_series(
    year: int = DEFAULTS.year,
    meeting: str = DEFAULTS.meeting,
    session: str = DEFAULTS.session,
) -> None:
    df = client.get(
        model=PitStopSeries,
        year=year,
        meeting=meeting,
        session=SessionSubType.parse(session),
    )
    print(df)


@app.command()
def lap_series(
    year: int = DEFAULTS.year,
    meeting: str = DEFAULTS.meeting,
    session: str = DEFAULTS.session,
) -> None:
    df = client.get(
        model=LapSeries,
        year=year,
        meeting=meeting,
        session=SessionSubType.parse(session),
    )
    print(df)


@app.command()
def driver_race_info(
    year: int = DEFAULTS.year,
    meeting: str = DEFAULTS.meeting,
    session: str = DEFAULTS.session,
) -> None:
    df = client.get(
        model=DriverRaceInfo,
        year=year,
        meeting=meeting,
        session=SessionSubType.parse(session),
    )
    print(df)


@app.command()
def driver_list(
    year: int = DEFAULTS.year,
    meeting: str = DEFAULTS.meeting,
    session: str = DEFAULTS.session,
) -> None:
    df = client.get(
        model=DriverList,
        year=year,
        meeting=meeting,
        session=SessionSubType.parse(session),
    )
    print(df)


@app.command()
def championship_prediction(
    year: int = DEFAULTS.year,
    meeting: str = DEFAULTS.meeting,
    session: str = DEFAULTS.session,
) -> None:
    df = client.get(
        model=ChampionshipPrediction,
        year=year,
        meeting=meeting,
        session=SessionSubType.parse(session),
    )
    print(df)


@app.command()
def content_streams(
    year: int = DEFAULTS.year,
    meeting: str = DEFAULTS.meeting,
    session: str = DEFAULTS.session,
) -> None:
    df = client.get(
        model=ContentStreams,
        year=year,
        meeting=meeting,
        session=SessionSubType.parse(session),
    )
    print(df)


@app.command()
def lap_count(
    year: int = DEFAULTS.year,
    meeting: str = DEFAULTS.meeting,
    session: str = DEFAULTS.session,
) -> None:
    df = client.get(
        model=LapCount,
        year=year,
        meeting=meeting,
        session=SessionSubType.parse(session),
    )
    print(df)


@app.command()
def archive_status(
    year: int = DEFAULTS.year,
    meeting: str = DEFAULTS.meeting,
    session: str = DEFAULTS.session,
) -> None:
    df = client.get(
        model=ArchiveStatus,
        year=year,
        meeting=meeting,
        session=SessionSubType.parse(session),
    )
    print(df)


@app.command()
def driver_tracker(
    year: int = DEFAULTS.year,
    meeting: str = DEFAULTS.meeting,
    session: str = DEFAULTS.session,
) -> None:
    df = client.get(
        model=DriverTracker,
        year=year,
        meeting=meeting,
        session=SessionSubType.parse(session),
    )
    print(df)


if __name__ == "__main__":
    app()
