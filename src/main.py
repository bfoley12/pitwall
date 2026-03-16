import typer

from .api_handler.f1_client import F1Client

app = typer.Typer()


@app.command()
def main(
    year: int = 2023,
    session: str | None = None,
    file: str | None = None,
    meeting: str | None = None,
) -> None:
    value = F1Client().get_season(year=year)
    print(value)


if __name__ == "__main__":
    app()
