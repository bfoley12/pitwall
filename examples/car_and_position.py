import marimo

__generated_with = "0.22.0"
app = marimo.App(width="full")


@app.cell
def _():
    import plotly.express as px
    import polars as pl

    from pitwall import DirectClient

    return DirectClient, pl, px


@app.cell
def _(DirectClient):
    client = DirectClient()
    return (client,)


@app.cell
def _(client, pl):
    car_data = client.get(
        model="CarData", year=2026, meeting="Australia", session="Race"
    )
    car_df = car_data.stream.data
    car_df = car_df.with_columns(
        pl.col("timestamp").dt.total_seconds().alias("timestamp_s")
    )
    return (car_df,)


@app.cell
def _(client):
    position_data = client.get(
        model="Position", year=2026, meeting="Australia", session="Race"
    )
    position_df = position_data.df
    return (position_df,)


@app.cell
def _(car_df, position_df):
    joined_df = car_df.join_asof(
        position_df, on="timestamp", by="racing_number", strategy="nearest"
    )
    return (joined_df,)


@app.cell
def _(joined_df, pl, px) -> None:
    px.line_3d(
        data_frame=joined_df.filter(pl.col("racing_number") == 44),
        x="x",
        y="y",
        z="speed",
        color="racing_number",
    )
    return


@app.cell
def _() -> None:
    return


if __name__ == "__main__":
    app.run()
