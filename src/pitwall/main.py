import typer

from pitwall.api_handler.f1_client import F1Client
from pitwall.api_handler.models.session import SessionSubType

app = typer.Typer()


@app.command()
def main(
    year: int = 2023,
    meeting: str | None = None,
    session: str | None = None,
    #file: str | None = None,
) -> None:
    client = F1Client()

    if meeting is not None and session is not None:
        print(
            client.get_car_data(
                year=year, meeting=meeting, session=SessionSubType.parse(session)
            )
        )


if __name__ == "__main__":
    app()
