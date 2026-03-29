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

## Data Description
- ArchiveStatus
  - Keyframe: signal for whether the archiving is complete
  - Stream: status of the archiving process
- CarData
  - Keyframe: 
  - Stream: 
## Usage
### DirectClient/AsyncDirectClient
temp text