# pitwall
## Motivation
The F1 livetiming API at livetiming.formula1.com exposes a rich set of data feeds - car telemetry, position data, timing, tyre stints, race control messages, weather, pit lane times, and more - going back to 2018. Existing tools access parts of this API but make tradeoffs that aren't right for every use case.

FastF1 is an excellent analysis-first library, but it consumes raw feeds internally and surfaces its own higher-level abstractions built on Pandas. While they deliver high quality, processed views of the data, some power-analysts may want to handle the raw data directly.

OpenF1 is a hosted REST API that proxies a subset of the feed data into simplified JSON endpoints. It's convenient for quick queries but only covers 2023 onward and restructures the data into its own schema.

pitwall takes a different approach. It maps the livetiming feeds directly, preserving their original structure while wrapping them in Pydantic V2 models and Polars DataFrames. The raw data stays intact - pitwall doesn't decide what's relevant or how fields should be combined. It gives you typed, validated access to the feeds as they exist, on a modern stack that's faster and more ergonomic than Pandas.

Like its namesake, pitwall sits close to the action - just one layer above the raw data.

## Supported Feeds


## Data Model


## Getting Started

### Install
```bash
# uv
uv add pitwall

# pip
pip install pitwall
```

### Discovery
```python
from pitwall import DirectClient

with DirectClient() as client:
    # Available years
    client.get_available_seasons()
    
    # Get meetings from specific year
    season = client.get_season(year=2026) # Using convenience method
    season.keyframe.meetings
    season.meetings # Aliases season.keyframe.meetings for convenience
    
    # Get sessions from specific meeting
    meeting = season.get_meeting(meeting="Australia")
    meeting.sessions
    
    # Get a specific session
    meeting.get_session(name="Qualifying")
    # Using convenience properties
    meeting.fp1 # Free Practice 1
    meeting.q # Qualifying
    
    # Get session directly and look at available data
    session_index = client.get(year=2026, meeting="Australia", session="Qualifying")
    session_index.available_feeds
```

### Get data
#### Synchronously
```python
from pitwall import DirectClient

client = DirectClient()

# Can also use DirectClient as a context manager:
# with DirectClient() as client:
#   ...

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
        client.get("CarData", year=2024, meeting="Monza", session="Race"),
        client.get("Position", year=2024, meeting="Monza", session="Race"),
    )
car_df = car_data.df
position_df = position.df

joined_df = car_df.join_asof(
    position_df, on="timestamp", by="racing_number", strategy="nearest"
)
```