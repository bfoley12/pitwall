from dataclasses import dataclass
import polars as pl

import typer

from pitwall.api_handler.f1_client import F1Client
from pitwall.api_handler.models.session import SessionSubType

app = typer.Typer()


@dataclass(frozen=True)
class Defaults:
    year: int = 2023
    meeting: str = "Yas Marina"
    session: str = "Race"


DEFAULTS = Defaults()
client = F1Client()


@app.command()
def season(year: int = DEFAULTS.year) -> None:
    print(client.get_season(year=year))


@app.command()
def meeting(year: int = DEFAULTS.year, name: str = DEFAULTS.meeting) -> None:
    res = client.get_meeting(year=year, meeting=name)
    print(res.sessions[0])


@app.command()
def session(
    year: int = DEFAULTS.year,
    meeting: str = DEFAULTS.meeting,
    name: str = DEFAULTS.session,
) -> None:
    print(
        client.get_session(
            year=year, meeting=meeting, session=SessionSubType.parse(name)
        )
    )


@app.command()
def session_index(
    year: int = DEFAULTS.year,
    meeting: str = DEFAULTS.meeting,
    name: str = DEFAULTS.session,
) -> None:
    print(
        client.get_session_feeds(
            year=year, meeting=meeting, session=SessionSubType.parse(name)
        )
    )


@app.command()
def timing(
    year: int = DEFAULTS.year,
    meeting: str = DEFAULTS.meeting,
    session: str = DEFAULTS.session,
) -> None:
    print(
        client.get_timing(
            year=year, meeting=meeting, session=SessionSubType.parse(session)
        )
    )


@app.command()
def timing_stats(
    year: int = DEFAULTS.year,
    meeting: str = DEFAULTS.meeting,
    session: str = DEFAULTS.session,
) -> None:
    print(
        client.get_timing_stats(
            year=year, meeting=meeting, session=SessionSubType.parse(session)
        )
    )


@app.command()
def car_data(
    year: int = DEFAULTS.year,
    meeting: str = DEFAULTS.meeting,
    session: str = DEFAULTS.session,
) -> None:
    df = client.get_car_data(
        year=year, meeting=meeting, session=SessionSubType.parse(session)
    )
    print(df)


@app.command()
def position_data(
    year: int = DEFAULTS.year,
    meeting: str = DEFAULTS.meeting,
    session: str = DEFAULTS.session,
) -> None:
    print(
        client.get_position_data(
            year=year, meeting=meeting, session=SessionSubType.parse(session)
        )
    )


@app.command()
def driver_info(
    year: int = DEFAULTS.year,
    meeting: str = DEFAULTS.meeting,
    session: str = DEFAULTS.session,
) -> None:
    print(
        client.get_driver_info(
            year=year, meeting=meeting, session=SessionSubType.parse(session)
        )
    )


@app.command()
def weather_data(
    year: int = DEFAULTS.year,
    meeting: str = DEFAULTS.meeting,
    session: str = DEFAULTS.session,
) -> None:
    print(
        client.get_weather_data(
            year=year, meeting=meeting, session=SessionSubType.parse(session)
        )
    )


@app.command()
def weather_data_series(
    year: int = DEFAULTS.year,
    meeting: str = DEFAULTS.meeting,
    session: str = DEFAULTS.session,
) -> None:
    print(
        client.get_weather_data_series(
            year=year, meeting=meeting, session=SessionSubType.parse(session)
        )
    )


@app.command()
def get_current_tyre(
    year: int = DEFAULTS.year,
    meeting: str = DEFAULTS.meeting,
    session: str = DEFAULTS.session,
) -> None:
    print(
        client.get_current_tyre(
            year=year, meeting=meeting, session=SessionSubType.parse(session)
        )
    )


@app.command()
def get_tyre_stints(
    year: int = DEFAULTS.year,
    meeting: str = DEFAULTS.meeting,
    session: str = DEFAULTS.session,
) -> None:
    print(
        client.get_tyre_stints(
            year=year, meeting=meeting, session=SessionSubType.parse(session)
        )
    )


@app.command()
def get_rcm(
    year: int = DEFAULTS.year,
    meeting: str = DEFAULTS.meeting,
    session: str = DEFAULTS.session,
) -> None:
    print(
        client.get_rcm(
            year=year, meeting=meeting, session=SessionSubType.parse(session)
        )
    )


@app.command()
def track_status(
    year: int = DEFAULTS.year,
    meeting: str = DEFAULTS.meeting,
    session: str = DEFAULTS.session,
) -> None:
    df = client.get_track_status(
        year=year, meeting=meeting, session=SessionSubType.parse(session)
    )
    print(df)


@app.command()
def pitstops(
    year: int = DEFAULTS.year,
    meeting: str = DEFAULTS.meeting,
    session: str = DEFAULTS.session,
) -> None:
    df = client.get_pit_stops(
        year=year, meeting=meeting, session=SessionSubType.parse(session)
    )
    print(df)


@app.command()
def lap_series(
    year: int = DEFAULTS.year,
    meeting: str = DEFAULTS.meeting,
    session: str = DEFAULTS.session,
) -> None:
    df = client.get_lap_series(
        year=year, meeting=meeting, session=SessionSubType.parse(session)
    )
    print(df)


@app.command()
def timing_app(
    year: int = DEFAULTS.year,
    meeting: str = DEFAULTS.meeting,
    session: str = DEFAULTS.session,
) -> None:
    df = client.get_timing_app(
        year=year, meeting=meeting, session=SessionSubType.parse(session)
    )
    print(df)

@app.command()
def driver_race_info(
    year: int = DEFAULTS.year,
    meeting: str = DEFAULTS.meeting,
    session: str = DEFAULTS.session,
) -> None:
    df = client.get_driver_race_info(
        year=year, meeting=meeting, session=SessionSubType.parse(session)
    )
    print(df)

@app.command()
def championship_prediction(
    year: int = DEFAULTS.year,
    meeting: str = DEFAULTS.meeting,
    session: str = DEFAULTS.session,
) -> None:
    df = client.get_championship_prediction(
        year=year, meeting=meeting, session=SessionSubType.parse(session)
    )
    print(df)

@app.command()
def championship_prediction_stream(
    year: int = DEFAULTS.year,
    meeting: str = DEFAULTS.meeting,
    session: str = DEFAULTS.session,
) -> None:
    df = client.get_championship_prediction_stream(
        year=year, meeting=meeting, session=SessionSubType.parse(session)
    )
    print(df)

if __name__ == "__main__":
    app()
