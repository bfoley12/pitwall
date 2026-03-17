import typer

from dataclasses import dataclass

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
    print(client.get_meeting(year=year, meeting=name))


@app.command()
def session(year: int = DEFAULTS.year, meeting: str = DEFAULTS.meeting, name: str = DEFAULTS.session) -> None:
    print(client.get_session(year=year, meeting=meeting, session=SessionSubType.parse(name)))


@app.command()
def timing(year: int = DEFAULTS.year, meeting: str = DEFAULTS.meeting, session: str = DEFAULTS.session) -> None:
    print(client.get_timing(year=year, meeting=meeting, session=SessionSubType.parse(session)))


@app.command()
def car_data(year: int = DEFAULTS.year, meeting: str = DEFAULTS.meeting, session: str = DEFAULTS.session) -> None:
    df = client.get_car_data(year=year, meeting=meeting, session=SessionSubType.parse(session))
    print(df)

@app.command()
def position_data(year: int = DEFAULTS.year, meeting: str = DEFAULTS.meeting, session: str = DEFAULTS.session) -> None:
    df = client.get_position_data(year=year, meeting=meeting, session=SessionSubType.parse(session))
    print(df)


if __name__ == "__main__":
    app()