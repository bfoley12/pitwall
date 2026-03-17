import typer

from pitwall.api_handler.f1_client import F1Client
from pitwall.api_handler.models.session import SessionSubType

app = typer.Typer()


@app.command()
def main(
    year: int = 2023,
    meeting: str | None = None,
    session: str | None = None,
    file: str | None = None,
) -> None:
    client = F1Client()

    if meeting is None:
        value = client.get_season(year=year)
    elif session is None:
        value = client.get_meeting(year=year, meeting=meeting)
    elif file is None:
        value = client.get_session(
            year=year, meeting=meeting, session=SessionSubType(session)
        )
    else:
        value = client.get_file(
            year=year, meeting=meeting, session=SessionSubType(session), file=file
        )

    print(value)


if __name__ == "__main__":
    app()
