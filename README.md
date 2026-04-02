# pitwall
## Introduction
pitwall brings a modern stack to the Formula 1 livetiming API. The loading and formatting of data relies on Pydantic json parsing combined with polars for fast, efficient transforms and analysis of data.

Users may get data in a two ways:
1. DirectClient/AsyncDirectClient
  - An unopinionated (as much as is possible) wrapper over the livetiming API
  - Uncleaned, untransformed data
  - Great for hobbyists that want to build their own, higher-level API and parsing logic
2. F1Client (unimplemented - actively developing)
  - Provides a legible interface to the underlying data
    - methods such as get_overtakes, get_stints, etc.
    - transforms and formats the raw data according to the method and intended use

## Getting Started

```
from pitwall import DirectClient

client = DirectClient()

# Request CarData and Position from livetiming API
car_data = client.get(model="CarData", year=2026, meeting="Shanghai", session="Race")
position_data = client.get(model="Position", year=2026, meeting="Shanghai", session="Race")

car_df = car_data.df
position_df = car_data.df

joined_df = car_df.join_asof()

```
