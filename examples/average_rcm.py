import marimo

__generated_with = "0.22.0"
app = marimo.App(width="full")


@app.cell
def _():
    import plotly.express as px
    import polars as pl

    from pitwall import DirectClient

    return (DirectClient,)


@app.cell
def _(DirectClient):
    client = DirectClient()
    return (client,)


@app.cell
def _(client):
    season = client.get_season(year=2026)
    return (season,)


@app.cell
def _(season):
    season.keyframe.meetings[0].sessions[0]
    return


@app.cell
def _(client):
    client.get(year=2026, model="Season").keyframe.meetings
    return


@app.cell
def _(client, season):
    data_list = []
    for meeting in season.keyframe.meetings:
        for session in meeting.sessions:
            print(session)
            data_list.append(client.get(session=session, model="RaceControlMessages"))
    return


@app.cell
def _(client):
    client.get(year=2026, meeting="Shanghai", session="Race", model="TlaRcm")
    return


@app.cell
async def _():
    import asyncio

    from pitwall import AsyncDirectClient

    # Use the async client as a context manager
    async with AsyncDirectClient() as async_client:
        # Launch multiple jobs at the same time (this sends 4 requests - 1 for keyframe and 1 for stream in each client.get)
        car_data, _position = await asyncio.gather(
            async_client.get("CarData", year=2024, meeting="Monza", session="Race"),
            async_client.get("Position", year=2024, meeting="Monza", session="Race"),
        )
    car_df = car_data.df
    position_df = car_data.df

    car_df.join_asof(
        position_df, on="timestamp", by="racing_number", strategy="nearest"
    )
    return (AsyncDirectClient,)


@app.cell
async def _(AsyncDirectClient):
    from pprint import pprint

    async with AsyncDirectClient() as _client:
        pprint(await _client.get_available_seasons())
    return


@app.cell
def _(DirectClient):
    _client = DirectClient()
    _meeting = _client.get_meeting(year=2026, meeting="Australia")
    _client.get(year=2026, meeting="Australia", session="Qualifying").available_feeds
    return


if __name__ == "__main__":
    app.run()
