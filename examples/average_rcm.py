import marimo

__generated_with = "0.22.0"
app = marimo.App(width="full")


@app.cell
def _():
    from pitwall import DirectClient
    import polars as pl
    import plotly.express as px

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
def _():
    return


if __name__ == "__main__":
    app.run()
