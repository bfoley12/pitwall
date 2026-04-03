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
def _(client):
    print(client.get_models(True))
    return


if __name__ == "__main__":
    app.run()
