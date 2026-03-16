# F1 Live Timing API — Endpoint Map

**Base URL:** `https://livetiming.formula1.com/static/`

> This is an undocumented, unofficial API. No auth required for archived (post-session) data.
> All paths are appended to the base URL above.

---

## 1. Discovery / Index Endpoints

These let you navigate the hierarchy: Year → Meeting → Session → Data files.

| Endpoint | Description |
|---|---|
| `Index.json` | Lists all available years |
| `{year}/Index.json` | Lists all meetings (race weekends) for a year |
| `{year}/{meeting_path}/Index.json` | Lists all sessions within a meeting |
| `{year}/{meeting_path}/{session_path}/Index.json` | Lists all available data files for a session |
| `SessionInfo.json` | Returns the current/most recent session globally |

### Path format

Paths follow this pattern:
```
{year}/{date}_{Meeting_Name}/{date}_{Session_Name}/
```

**Example (2026 Australian GP Race):**
```
2026/2026-03-08_Australian_Grand_Prix/2026-03-08_Race/
```

**Example (2026 Pre-Season Testing Day 3):**
```
2026/2026-02-20_Pre-Season_Testing/2026-02-20_Day_3/
```

> **Important:** The date in the path is the date of the *session*, not the meeting start.
> Naming can be inconsistent — always discover paths via `Index.json` rather than guessing.

### curl examples — start here
```bash
# What years are available?
curl "https://livetiming.formula1.com/static/Index.json"

# What meetings happened in 2026?
curl "https://livetiming.formula1.com/static/2026/Index.json"

# What's the current/latest session?
curl "https://livetiming.formula1.com/static/SessionInfo.json"

# What data files exist for a specific session?
# (get the exact path from the year/meeting index first)
curl "https://livetiming.formula1.com/static/2026/2026-03-08_Australian_Grand_Prix/2026-03-08_Race/Index.json"
```

---

## 2. Session Metadata

All paths below are relative to a session base path, e.g.:
`https://livetiming.formula1.com/static/2026/2026-03-08_Australian_Grand_Prix/2026-03-08_Race/`

| Endpoint | Format | Description |
|---|---|---|
| `SessionInfo.json` | JSON | Session metadata: meeting name, circuit, session type, start/end times, GMT offset |
| `ArchiveStatus.json` | JSON | Whether post-session archival is complete (`"Complete"` or `"Generating"`) |
| `ContentStreams.json` | JSON | Available audio/video content stream references |
| `DriverList.json` | JSON | All drivers: number, abbreviation, name, team, team colour, headshot URL |
| `SessionData.jsonStream` | jsonStream | Session status changes (started, finished, aborted, etc.) |
| `ExtrapolatedClock.jsonStream` | jsonStream | Estimated remaining session time (extrapolated from server) |

---

## 3. Timing & Lap Data

| Endpoint | Format | Description |
|---|---|---|
| `TimingData.jsonStream` | jsonStream | **The big one.** Sector times, lap times, speed traps, gaps, intervals — mixed stream |
| `TimingDataF1.jsonStream` | jsonStream | Formatted version of TimingData (pre-processed for display) |
| `TimingAppData.jsonStream` | jsonStream | Stint info, tyre compound, tyre age, grid position |
| `TimingStats.jsonStream` | jsonStream | Best/last sector times, personal bests, speed trap bests |
| `LapCount.jsonStream` | jsonStream | Current lap number and total laps |
| `LapSeries.jsonStream` | jsonStream | Cumulative lap time series per driver |
| `TopThree.jsonStream` | jsonStream | Current top 3 positions (used for broadcast graphics) |
| `CurrentTyres.jsonStream` | jsonStream | Current tyre compound and age for each driver |

---

## 4. Car Telemetry & Position

| Endpoint | Format | Description |
|---|---|---|
| `CarData.z.jsonStream` | **zlib compressed** jsonStream | Telemetry: speed, RPM, gear, throttle %, brake %, DRS. ~240ms sample rate |
| `Position.z.jsonStream` | **zlib compressed** jsonStream | Car X/Y/Z coordinates on track. ~220ms sample rate |

### Decompression

The `.z.` files are zlib-compressed. To read them:

```python
import zlib
import json

# Each line in the jsonStream is: timestamp + jsondata
# The json data portion is base64-encoded zlib
import base64

compressed_data = base64.b64decode(raw_value)
decompressed = zlib.decompress(compressed_data, -zlib.MAX_WBITS)
parsed = json.loads(decompressed)
```

### Telemetry channels (CarData)

| Channel | Type | Description |
|---|---|---|
| `Speed` | int | km/h |
| `RPM` | int | Engine RPM |
| `Gear` | int | Current gear (0 = neutral) |
| `Throttle` | int | 0–100% |
| `Brake` | int | 0 or 100 (binary on/off in the feed) |
| `DRS` | int | DRS status flags |

### Position channels

| Channel | Type | Description |
|---|---|---|
| `X` | float | Track X coordinate |
| `Y` | float | Track Y coordinate |
| `Z` | float | Track Z coordinate (elevation) |
| `Status` | string | On track / Off track |

---

## 5. Race Control & Track Status

| Endpoint | Format | Description |
|---|---|---|
| `RaceControlMessages.jsonStream` | jsonStream | Flags, penalties, investigations, DRS enabled/disabled, VSC, SC messages |
| `TrackStatus.jsonStream` | jsonStream | Track status codes: `1`=Clear, `2`=Yellow, `3`=??, `4`=SC, `5`=Red, `6`=VSC, `7`=VSC Ending |
| `TeamRadio.jsonStream` | jsonStream | Team radio clip references (URLs to audio files) |

---

## 6. Weather

| Endpoint | Format | Description |
|---|---|---|
| `WeatherData.jsonStream` | jsonStream | Air temp, track temp, humidity, pressure, wind speed/direction, rainfall (bool) |

---

## 7. Championship & Scoring

| Endpoint | Format | Description |
|---|---|---|
| `DriverRaceInfo.jsonStream` | jsonStream | Per-driver race info (overtake count, positions gained/lost) |
| `ChampionshipPrediction.jsonStream` | jsonStream | Live championship points projections |
| `DriverScore.jsonStream` | jsonStream | Driver performance scores (G-force, throttle, steering, braking) |
| `PitLaneTimeCollection.jsonStream` | jsonStream | Pit stop durations and pit lane times |

---

## 8. Data Formats

### `.json` — Static JSON
Standard JSON. One-time snapshot. `curl` it and pipe to `jq`.

### `.jsonStream` — Newline-delimited timestamped JSON
Each line contains a timestamp and a JSON payload, separated by format-specific delimiters.
The stream represents the full history of changes during the session.

```
# Typical line structure (varies by endpoint):
{"Timestamp":"2026-03-08T04:02:31.123Z","Data":{...}}
```

> Lines may not be valid JSON individually — FastF1's parser handles edge cases
> like single quotes instead of double quotes, `True`/`False` instead of `true`/`false`.

### `.z.jsonStream` — Compressed jsonStream
Same as jsonStream but each data payload is base64-encoded zlib.
Used for high-frequency data (CarData, Position) to reduce bandwidth.

---

## 9. Quick Start: Fetch the 2026 Australian GP Race Telemetry

```bash
# Step 1: Discover the session path
curl -s "https://livetiming.formula1.com/static/2026/Index.json" | jq .

# Step 2: Get available data files for that session
# (use the path from Step 1)
curl -s "https://livetiming.formula1.com/static/2026/2026-03-08_Australian_Grand_Prix/2026-03-08_Race/Index.json" | jq .

# Step 3: Fetch specific data
# Driver list
curl -s "https://livetiming.formula1.com/static/2026/2026-03-08_Australian_Grand_Prix/2026-03-08_Race/DriverList.json" | jq .

# Timing data (large file)
curl -s "https://livetiming.formula1.com/static/2026/2026-03-08_Australian_Grand_Prix/2026-03-08_Race/TimingData.jsonStream" > timing.jsonStream

# Weather
curl -s "https://livetiming.formula1.com/static/2026/2026-03-08_Australian_Grand_Prix/2026-03-08_Race/WeatherData.jsonStream" > weather.jsonStream

# Race control messages
curl -s "https://livetiming.formula1.com/static/2026/2026-03-08_Australian_Grand_Prix/2026-03-08_Race/RaceControlMessages.jsonStream" > racecontrol.jsonStream
```

---

## 10. Python Quick Start (without FastF1)

```python
import requests
import json

BASE = "https://livetiming.formula1.com/static"

# Get year index
meetings = requests.get(f"{BASE}/2026/Index.json").json()
for m in meetings.get("Meetings", []):
    print(f"{m['Name']}: {m['Path']}")

# Get session index
session_path = "2026/2026-03-08_Australian_Grand_Prix/2026-03-08_Race"
index = requests.get(f"{BASE}/{session_path}/Index.json").json()
print("Available feeds:", list(index.get("Feeds", {}).keys()))

# Fetch driver list
drivers = requests.get(f"{BASE}/{session_path}/DriverList.json").json()
for num, info in drivers.items():
    print(f"#{num} {info.get('Tla', '???')} - {info.get('TeamName', '???')}")

# Fetch weather data
weather_raw = requests.get(f"{BASE}/{session_path}/WeatherData.jsonStream").text
for line in weather_raw.strip().split("\n"):
    # Each line has a timestamp prefix followed by JSON
    # Parsing varies — some lines are partial updates
    print(line[:120])
```

---

## 11. Live Streaming (SignalR) — Requires Auth

For **live** data during an active session (not archived):

| Detail | Value |
|---|---|
| WebSocket URL | `wss://livetiming.formula1.com/signalrcore` |
| Negotiate URL | `https://livetiming.formula1.com/signalrcore/negotiate` |
| Protocol | SignalR Core (ASP.NET Core) |
| Auth required | Yes — F1 TV account (any tier) |
| Library | `signalrcore` (Python) |

FastF1 ≥3.7 handles this via `fastf1.livetiming.client.SignalRClient`.
The auth token is obtained via `fastf1.internals.f1auth.get_auth_token()`.

**The archived static endpoints above are the practical choice for most use cases.**
Data is typically available 30–120 minutes after a session ends.

---

## 12. Notes & Gotchas

- **No official documentation exists.** This is all reverse-engineered.
- **Path names are inconsistent.** Pre-season testing uses `Day_1`, `Day_2`, `Day_3` instead of `Practice_1`, etc. Sprint weekends have different session names. Always discover via `Index.json`.
- **Data availability:** ~30–120 min after session end. Check `ArchiveStatus.json` — it should say `"Complete"`.
- **No CORS headers.** You can't fetch from browser JavaScript directly. Use a server-side proxy or a backend.
- **Rate limiting:** No known hard limits, but be reasonable. Cache aggressively.
- **jsonStream parsing is tricky.** Lines may contain Python-style booleans (`True`/`False`), single quotes, or other non-standard JSON. FastF1's parser handles this; if rolling your own, sanitise first.
- **CarData and Position samples don't align.** They have different sample rates (~240ms vs ~220ms). Interpolation/resampling is needed to merge them.