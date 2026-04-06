# pitwall
## Motivation
The F1 livetiming API at https://livetiming.formula1.com exposes a rich set of data feeds - car telemetry, position data, timing, tyre stints, race control messages, weather, pit lane times, and more - going back to 2018. Existing tools access parts of this API but make tradeoffs that aren't right for every use case.

FastF1 is an excellent analysis-first library, but it consumes raw feeds internally and surfaces its own higher-level abstractions built on Pandas. While they deliver high quality, processed views of the data, some power-analysts may want to handle the raw data directly.

OpenF1 is a hosted REST API that proxies a subset of the feed data into simplified JSON endpoints. It's convenient for quick queries but only covers 2023 onward and restructures the data into its own schema.

pitwall takes a different approach. It maps the livetiming feeds directly, preserving their original structure while wrapping them in Pydantic V2 models and Polars DataFrames. The raw data stays intact - pitwall doesn't decide what's relevant or how fields should be combined. It gives you typed, validated access to the feeds as they exist, on a modern stack that's faster and more ergonomic than Pandas.

Like its namesake, pitwall sits close to the action - just one layer above the raw data.

## Supported Feeds
<details>
<summary>Supported Feeds (32)</summary>

| Feed | Keyframe (`.json`) | Stream (`.jsonStream`) |
|------|:-------:|:-------------:|
| TimingDataF1 | ✅ | ✅ |
| TimingStats | ✅ | ✅ |
| TimingAppData | ✅ | ✅ |
| LapSeries | ✅ | ✅ |
| TyreStintSeries | ✅ | ✅ |
| DriverTracker | ✅ | ✅ |
| OvertakeSeries | ✅ | ✅ |
| PitStop | ✅ | ✅ |
| PitStopSeries | ✅ | ✅ |
| CurrentTyres | ✅ | ✅ |
| TimingData | ✅ | ✅ |
| LapCount | ✅ | ✅ |
| TopThree | ✅ | ✅ |
| CarData.z | ✅ | ✅ |
| Position.z | ✅ | ✅ |
| RaceControlMessages | ✅ | ✅ |
| TrackStatus | ✅ | ✅ |
| TlaRcm | ✅ | ✅ |
| TeamRadio | ✅ | ✅ |
| WeatherData | ✅ | ✅ |
| WeatherDataSeries | ✅ | ✅ |
| DriverList | ✅ | ✅ |
| PitLaneTimeCollection | ✅ | ✅ |
| ChampionshipPrediction | ✅ | ✅ |
| DriverRaceInfo | ✅ | ✅ |
| SessionInfo | ✅ | ✅ |
| SessionData | ✅ | ✅ |
| SessionStatus | ✅ | ✅ |
| ArchiveStatus | ✅ | ✅ |
| Heartbeat | ✅ | ✅ |
| ExtrapolatedClock | ✅ | ✅ |
| ContentStreams | ✅ | ✅ |
| AudioStreams | ✅ | ✅ |

</details>

## Data Model
The livetiming API is a hierarchical API that progressively provides more information. The base of the API is https://livetiming.formula1.com/static (from here on, we will refer to that as root - '/' and reference the API layers as starting from '/'). Most layers provides an Index.json that we can learn about the available endpoints to dive deeper into. For example, /Index.json shows the current year (why it doesn't show every available year is not known to me). /{year}/Index.json provides a list of meetings (race or testing weekends) with their relevant sessions (FP1, Qualifying, etc.). Oddly, /{year}/{meeting} does not provide an Index.json file. /{year}/{meeting}/{session}/Index.json displays all available 'feeds' for a session. A 'feed' refers to a single collection of data submitted either as a .json file (a 'keyframe') or a .jsonStream (aka .jsonl) file (a 'stream'). 

A keyframe represents a static json, while streams are dynamic, partial updates throughout the course of a session. This makes Pydantic a natural package to model the fields. Streams are recorded as .jsonStream (aka .jsonl) files, timestamped by the session time as a duration. Polars is used to represent the streamed data.

Consistency is prioritized across returned models. Each model contains a 'keyframe' and 'stream' (with the exceptions of Season, Meeting, and Session, since they only offer an Index.json file). It is up to the user to determine whether they need the data from the stream or the keyframe, as some feeds are represented better in the stream (ie. CarData and Position keyframes are not useful for many applications). In anticipation of many use cases relying on the stream's dataframe, a property is provided directly on the F1DataContainer object: obj.df.

## Getting Started

### Install
```bash
# uv
uv add pitwall

# pip
pip install pitwall
```

### Data Exploration
```python
from pitwall import DirectClient

with DirectClient() as client:
    # Available seasons
    client.get_available_seasons()

    # Get meetings from specific year
    season = client.get_season(year=2026) # Using convenience method
    season.meetings # Aliases season.keyframe.meetings for convenience

    # Get sessions from specific meeting
    meeting = season.get_meeting(meeting="Australia")
    meeting.sessions

    # Get a specific session
    meeting.get_session(name="Qualifying")
    # Using convenience properties
    meeting.q # Qualifying

    # Get session directly from client and look at available data
    session_index = client.get(year=2026, meeting="Australia", session="Qualifying")
    session_index.available_feeds
```

### Get data
#### Synchronously
```python
from pitwall import DirectClient


# Can also use DirectClient as a long-lived instance:
# client = DirectClient()
# val = client.get(...)

with DirectClient() as client:
    # Request CarData and Position from livetiming API
    car_data = client.get(model="CarData", year=2026, meeting="Shanghai", session="Race") # Returns a Keyframe+Stream of CarData
    position = client.get(model="Position", year=2026, meeting="Shanghai", session="Race") # Returns a Keyframe+Stream of Position

car_df = car_data.df
position_df = position.df

joined_df = car_df.join_asof(
    position_df, on="timestamp", by="racing_number", strategy="nearest"
)
```
#### Asynchronously

```python
import asyncio
from pitwall import AsyncDirectClient

# Use the async client as a context manager
async with AsyncDirectClient() as client:
    # Launch multiple jobs at the same time (this sends 4 requests - 1 for keyframe and 1 for stream in each client.get)
    car_data, position = await asyncio.gather(
        client.get(model="CarData", year=2024, meeting="Monza", session="Race"),
        client.get(model="Position", year=2024, meeting="Monza", session="Race"),
    )
car_df = car_data.df # Alias to car_data.stream.data
position_df = position.df

joined_df = car_df.join_asof(
    position_df, on="timestamp", by="racing_number", strategy="nearest"
)
```

## Modify (Async)DirectClient settings
Since your connection to livetiming may be different from the testing environment, we allow for customization of ClientSettings. Check settings::ClientSettings for what can be changed.
```python
from pitwall import AsyncDirectClient, ClientSettings

settings = ClientSettings(
    total_timeout = 10, # Set max allowed time to wait for client to 10s
    request_timeout = 2 # Set max allowed time to wait per request to 2s
)

async_client = AsyncDirectClient(settings=settings)
# Note: For DirectClient, total_timeout does not have any effect at the moment.
# Each request is still limited to request timeout, so the max waited for is 3x request_timeout
sync_client = DirectClient(settings=settings)

sync_client.get(year=2026)
```
