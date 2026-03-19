import base64
import json
import zlib
from typing import Any, Literal, cast

import httpx
import polars as pl

from pitwall.api_handler.models.base import F1Model, F1ModelT
from pitwall.api_handler.models.championship_prediction import ChampionshipPrediction, ChampionshipPredictionStream, build_championship_prediction_stream
from pitwall.api_handler.models.driver_list import DriverList
from pitwall.api_handler.models.driver_race_info import DriverRaceInfo, build_driver_race_info
from pitwall.api_handler.models.meeting import Meeting
from pitwall.api_handler.models.race_control_messages import RaceControlMessages
from pitwall.api_handler.models.season import Season
from pitwall.api_handler.models.session import (
    RACE_ONLY_SESSIONS,
    SessionIndex,
    SessionInfo,
    SessionSubType,
)
from pitwall.api_handler.models.timing_app import TimingApp
from pitwall.api_handler.models.timing_data import TimingDataF1
from pitwall.api_handler.models.timing_stats import TimingStats
from pitwall.api_handler.models.tyres import CurrentTyres
from pitwall.api_handler.models.weather_data import WeatherData
from pitwall.api_handler.path_resolver import PathResolver


_RACE_SESSIONS: frozenset[SessionSubType] = frozenset({
    SessionSubType.RACE,
    SessionSubType.SPRINT,
})

class PitStopBroadcastEvent:
    __slots__ = ("cleared_at", "displayed_at", "duration", "lap")

    def __init__(self, displayed_at: str, lap: str, duration: float | None) -> None:
        self.displayed_at = displayed_at
        self.cleared_at: str | None = None
        self.lap = lap
        self.duration: float | None = duration


PIT_STOP_SCHEMA = {
    "session_time": pl.Utf8,
    "car_number": pl.Utf8,
    "pit_stop_time": pl.Float64,
    "pit_lane_time": pl.Float64,
    "lap": pl.Int64,
    "utc": pl.Utf8,
    "broadcast_displayed_at": pl.Utf8,
    "broadcast_cleared_at": pl.Utf8,
}


class F1Client:
    def __init__(self) -> None:
        self.http: httpx.Client = httpx.Client()

    def get_season(self, year: int) -> Season:
        return self.fetch(model=Season, year=year)

    def get_meeting(self, year: int, meeting: str) -> Meeting:
        season = self.get_season(year=year)
        return season.get_meeting(meeting)

    def get_session(
        self, year: int, meeting: str, session: SessionSubType
    ) -> SessionInfo:
        return self.fetch(
            model=SessionInfo,
            year=year,
            meeting=meeting,
            session=session,
            file="SessionInfo.json",
        )

    def get_session_feeds(
        self, year: int, meeting: str, session: SessionSubType
    ) -> SessionIndex:
        return self.fetch(
            model=SessionIndex, year=year, meeting=meeting, session=session
        )

    def get_timing(
        self, year: int, meeting: str, session: SessionSubType
    ) -> TimingDataF1:
        return self.fetch(
            model=TimingDataF1,
            year=year,
            meeting=meeting,
            session=session,
            file="TimingDataF1.json",
        )

    def get_driver_info(
        self, year: int, meeting: str, session: SessionSubType
    ) -> DriverList:
        return self.fetch(
            model=DriverList,
            year=year,
            meeting=meeting,
            session=session,
            file="DriverList.json",
        )

    def get_car_data(
        self, year: int, meeting: str, session: SessionSubType
    ) -> pl.DataFrame:
        data = cast(
            dict[str, Any],
            self._fetch_raw(
                year=year, meeting=meeting, session=session, file="CarData.z.jsonStream"
            ),
        )
        rows = [
            {
                "utc": entry["Utc"],
                "car_number": car_num,
                "rpm": ch.get("0", 0),
                "speed": ch.get("2", 0),
                "gear": ch.get("3", 0),
                "throttle": ch.get("4", 0),
                "brake": ch.get("5", 0),
                "drs": ch.get("45"),
            }
            for entry in data["Entries"]
            for car_num, car in entry["Cars"].items()
            for ch in [car["Channels"]]
        ]
        return pl.DataFrame(rows).with_columns(
            pl.col("utc").str.to_datetime("%Y-%m-%dT%H:%M:%S%.fZ")
        )

    def get_position_data(
        self, year: int, meeting: str, session: SessionSubType
    ) -> pl.DataFrame:
        data = cast(
            dict[str, Any],
            self._fetch_raw(
                year=year,
                meeting=meeting,
                session=session,
                file="Position.z.jsonStream",
            ),
        )
        rows = [
            {
                "timestamp": pos["Timestamp"],
                "car_number": car_num,
                "status": entry["Status"],
                "x": entry["X"],
                "y": entry["Y"],
                "z": entry["Z"],
            }
            for pos in data["Position"]
            for car_num, entry in pos["Entries"].items()
        ]
        return pl.DataFrame(rows).with_columns(
            pl.col("timestamp").str.to_datetime("%Y-%m-%dT%H:%M:%S%.fZ")
        )

    def get_weather_data(
        self, year: int, meeting: str, session: SessionSubType
    ) -> WeatherData:
        return self.fetch(
            model=WeatherData,
            year=year,
            meeting=meeting,
            session=session,
            file="WeatherData.json",
        )

    def get_weather_data_series(
        self, year: int, meeting: str, session: SessionSubType
    ) -> pl.DataFrame:
        data = data = cast(
            dict[str, Any],
            self._fetch_raw(
                year=year,
                meeting=meeting,
                session=session,
                file="WeatherDataSeries.json",
            ),
        )
        rows = [
            {
                "timestamp": ser["Timestamp"],
                "air_temp": ser["Weather"].get("AirTemp"),
                "humidity": ser["Weather"].get("Humidity"),
                "pressure": ser["Weather"].get("Pressure"),
                "rainfall": ser["Weather"].get("Rainfall"),
                "track_temp": ser["Weather"].get("TrackTemp"),
            }
            for ser in data["Series"]
        ]
        return pl.DataFrame(rows).with_columns(
            pl.col("timestamp").str.to_datetime("%Y-%m-%dT%H:%M:%S%.fZ")
        )

    def get_current_tyre(
        self, year: int, meeting: str, session: SessionSubType
    ) -> CurrentTyres:
        return self.fetch(
            model=CurrentTyres,
            year=year,
            meeting=meeting,
            session=session,
            file="CurrentTyres.json",
        )

    def get_tyre_stints(
        self, year: int, meeting: str, session: SessionSubType
    ) -> pl.DataFrame:
        data = cast(
            dict[str, Any],
            self._fetch_raw(
                year=year, meeting=meeting, session=session, file="TyreStintSeries.json"
            ),
        )
        # TODO: think about adding on columns like stint_length (total_laps-start_laps) vs having it be calculated at needed site
        # Leaning towards keeping this a thin wrapper and letting callers handle it. Will need to document the columns
        rows = [
            {
                "car_number": car_num,
                "stint_number": stint_idx,
                "compound": stint["Compound"],
                "new": stint["New"] == "true",
                "tyres_not_changed": int(stint["TyresNotChanged"]),
                "total_laps": stint["TotalLaps"],
                # TODO: Clarify what StartLaps is (I assume it is laps done on tyre before this stint)
                "start_laps": stint["StartLaps"],
            }
            for car_num, stints in data["Stints"].items()
            for stint_idx, stint in enumerate(stints)
        ]
        return pl.DataFrame(rows).with_columns(
            pl.col("compound").cast(pl.Categorical),
        )

    def get_rcm(
        self, year: int, meeting: str, session: SessionSubType
    ) -> RaceControlMessages:
        return self.fetch(
            model=RaceControlMessages,
            year=year,
            meeting=meeting,
            session=session,
            file="RaceControlMessages.json",
        )

    def get_track_status(
        self, year: int, meeting: str, session: SessionSubType
    ) -> pl.DataFrame:
        data = cast(
            list[Any],
            self._fetch_raw(
                year=year,
                meeting=meeting,
                session=session,
                file="TrackStatus.jsonStream",
            ),
        )
        rows = [
            {
                "timestamp": entry["Timestamp"],
                "status": entry["Data"]["Status"],
                "message": entry["Data"]["Message"],
            }
            for entry in data
        ]
        return pl.DataFrame(rows)

    # TODO: Refactor method to make it more legible
    # TODO: Lots of Null pitstops at the end of the 2026 Shanghai sprint - looks like its the end of the race and cars returning to garage?
    # But GR (car 63) stopped around the same time and it was lap 13... Need to investigate
    # TODO: session_time and broadcast_displayed_at seem 100% identical. broadcast_displayed_at may be redundant
    # TODO: check if broadcast_cleared_at is accurate
    def get_pit_stops(
        self, year: int, meeting: str, session: SessionSubType
    ) -> pl.DataFrame:
        pit_stop_raw = self._try_fetch_raw(
            year=year, meeting=meeting, session=session, file="PitStop.jsonStream"
        )
        series_raw = self._try_fetch_raw(
            year=year, meeting=meeting, session=session, file="PitStopSeries.jsonStream"
        )
        collection_raw = self._try_fetch_raw(
            year=year,
            meeting=meeting,
            session=session,
            file="PitLaneTimeCollection.jsonStream",
        )

        if pit_stop_raw is None and collection_raw is None:
            return pl.DataFrame(schema=PIT_STOP_SCHEMA)

        # Build UTC lookup from PitStopSeries
        utc_lookup: dict[tuple[str, str], str] = {}
        if series_raw is not None:
            for entry in cast(list[Any], series_raw):
                pit_times = entry["Data"].get("PitTimes", {})
                for car_num, stops in pit_times.items():
                    if isinstance(stops, list):
                        for stop in stops:
                            key = (car_num, stop["PitStop"]["PitLaneTime"])
                            utc_lookup[key] = stop["Timestamp"]
                    elif isinstance(stops, dict):
                        for stop in stops.values():
                            if isinstance(stop, dict) and "PitStop" in stop:
                                key = (car_num, stop["PitStop"]["PitLaneTime"])
                                utc_lookup[key] = stop["Timestamp"]

        # Build broadcast lookup from PitLaneTimeCollection
        broadcast_events: dict[str, dict[str, list[PitStopBroadcastEvent]]] = {}
        if collection_raw is not None:
            pending: dict[str, PitStopBroadcastEvent] = {}
            for entry in cast(list[Any], collection_raw):
                pit_times = entry["Data"].get("PitTimes", {})
                timestamp = entry["Timestamp"]

                for car_num in pit_times.get("_deleted", []):
                    if car_num in pending:
                        event = pending.pop(car_num)
                        event.cleared_at = timestamp
                        broadcast_events.setdefault(car_num, {}).setdefault(
                            event.lap, []
                        ).append(event)

                for car_num, pit_data in pit_times.items():
                    if car_num == "_deleted":
                        continue
                    duration_str = pit_data.get("Duration", "")
                    duration = float(duration_str) if duration_str else None
                    pending[car_num] = PitStopBroadcastEvent(
                        displayed_at=timestamp,
                        lap=pit_data["Lap"],
                        duration=duration,
                    )

            for car_num, event in pending.items():
                broadcast_events.setdefault(car_num, {}).setdefault(
                    event.lap, []
                ).append(event)

        # Build rows from PitStop (primary) or PitLaneTimeCollection (fallback)
        rows: list[dict[str, object]] = []

        if pit_stop_raw is not None:
            for entry in cast(list[Any], pit_stop_raw):
                d = entry["Data"]
                car = d["RacingNumber"]
                lane_time = float(d["PitLaneTime"])
                lap = d.get("Lap")

                # UTC enrichment
                key = (car, d["PitLaneTime"])
                utc = utc_lookup.get(key)

                # Broadcast enrichment
                broadcast = None
                if lap is not None:
                    lap_broadcasts = broadcast_events.get(car, {}).get(str(lap), [])
                    if lap_broadcasts:
                        broadcast = lap_broadcasts.pop(0)

                rows.append(
                    {
                        "session_time": entry["Timestamp"],
                        "car_number": car,
                        "pit_stop_time": float(d["PitStopTime"])
                        if "PitStopTime" in d
                        else None,
                        "pit_lane_time": lane_time,
                        "lap": int(lap) if lap is not None else None,
                        "utc": utc,
                        "broadcast_displayed_at": broadcast.displayed_at
                        if broadcast
                        else None,
                        "broadcast_cleared_at": broadcast.cleared_at
                        if broadcast
                        else None,
                    }
                )
        else:
            # Fallback: reconstruct from PitLaneTimeCollection only
            for car_num, laps in broadcast_events.items():
                for lap, events in laps.items():
                    for event in events:
                        rows.append(
                            {
                                "session_time": event.displayed_at,
                                "car_number": car_num,
                                "pit_stop_time": None,
                                "pit_lane_time": event.duration,
                                "lap": int(lap),
                                "utc": None,
                                "broadcast_displayed_at": event.displayed_at,
                                "broadcast_cleared_at": event.cleared_at,
                            }
                        )

        df = pl.DataFrame(rows, schema=PIT_STOP_SCHEMA)
        return df.with_columns(
            pl.col("utc").str.to_datetime("%Y-%m-%dT%H:%M:%S%.fZ", strict=False),
        )

    def get_lap_series(
        self,
        year: int,
        meeting: str,
        session: SessionSubType,
    ) -> pl.DataFrame:
        data = cast(
            dict[str, Any],
            self._fetch_raw(
                year=year, meeting=meeting, session=session, file="LapSeries.json"
            ),
        )
        rows = [
            {
                "car_number": data["RacingNumber"],
                "lap_number": lap_number,
                "position": position,
            }
            for index, data in data.items()
            for lap_number, position in enumerate(data["LapPosition"])
        ]
        return pl.DataFrame(rows)

    def get_timing_stats(
        self, year: int, meeting: str, session: SessionSubType
    ) -> TimingStats:
        return self.fetch(
            model=TimingStats,
            year=year,
            meeting=meeting,
            session=session,
            file="TimingStats.json",
        )

    def get_timing_app(
        self, year: int, meeting: str, session: SessionSubType
    ) -> TimingApp:
        return self.fetch(
            model=TimingApp,
            year=year,
            meeting=meeting,
            session=session,
            file="TimingAppData.json",
        )

    def get_driver_race_info(
        self, year: int, meeting: str, session: SessionSubType,
    ) -> DriverRaceInfo:
        data = cast(
            list[Any],
            self._fetch_raw(
                year=year, meeting=meeting, session=session,
                file="DriverRaceInfo.jsonStream",
            ),
        )
        return build_driver_race_info(data)

    def get_championship_prediction(
        self, year: int, meeting: str, session: SessionSubType,
    ) -> ChampionshipPrediction:
        if session not in RACE_ONLY_SESSIONS:
            msg = f"ChampionshipPrediction is only available for Race/Sprint sessions, got {session}"
            raise ValueError(msg)
        return self.fetch(
            year=year, meeting=meeting, session=session,
            file="ChampionshipPrediction.json",
            model=ChampionshipPrediction,
        )

    def get_championship_prediction_stream(
        self, year: int, meeting: str, session: SessionSubType,
    ) -> ChampionshipPredictionStream:
        if session not in RACE_ONLY_SESSIONS:
            msg = f"ChampionshipPrediction is only available for Race/Sprint sessions, got {session}"
            raise ValueError(msg)
        data = cast(
            list[Any],
            self._fetch_raw(
                year=year, meeting=meeting, session=session,
                file="ChampionshipPrediction.jsonStream",
            ),
        )
        return build_championship_prediction_stream(data)

    def get_file(
        self, year: int, meeting: str, session: SessionSubType, file: str
    ) -> F1Model:
        return self.fetch(
            model=F1Model, year=year, meeting=meeting, session=session, file=file
        )

    def fetch(
        self,
        model: type[F1ModelT],
        year: int | None = None,
        meeting: str | None = None,
        session: SessionSubType | None = None,
        file: str = "Index.json",
    ) -> F1ModelT:
        url = PathResolver(year=year, meeting=meeting, session=session, file=file).url
        response = self.http.get(url)
        _ = response.raise_for_status()
        data = self._decode_response(response, file)
        return model.model_validate(data)

    def _fetch_raw(
        self,
        year: int | None = None,
        meeting: str | None = None,
        session: SessionSubType | None = None,
        file: str = "Index.json",
    ) -> object:
        url = PathResolver(year=year, meeting=meeting, session=session, file=file).url
        response = self.http.get(url)
        _ = response.raise_for_status()
        return self._decode_response(response, file)

    def _try_fetch_raw(
        self,
        year: int | None = None,
        meeting: str | None = None,
        session: SessionSubType | None = None,
        file: str = "Index.json",
    ) -> object | None:
        try:
            return self._fetch_raw(
                year=year, meeting=meeting, session=session, file=file
            )
        except httpx.HTTPStatusError:
            return None

    def _decode_response(self, response: httpx.Response, file: str) -> object:
        text = response.text.lstrip("\ufeff")

        if file.endswith(".z.jsonStream"):
            return self._decode_compressed_stream(text)

        if file.endswith(".jsonStream"):
            entries: list[object] = []
            for line in text.strip().split("\n"):
                if not line:
                    continue
                brace_idx = line.index("{")
                timestamp = line[:brace_idx]
                payload = json.loads(line[brace_idx:])
                entries.append({"Timestamp": timestamp, "Data": payload})
            return entries

        if ".z." in file:
            raw = json.loads(text)
            decoded = base64.b64decode(raw + "==")
            decompressed = zlib.decompress(decoded, -zlib.MAX_WBITS)
            return json.loads(decompressed)

        return json.loads(text)

    def _decode_compressed_stream(self, text: str) -> dict[str, list[object]]:
        collected: dict[str, list[object]] = {}
        for line in text.strip().split("\n"):
            if not line:
                continue
            quote_idx = line.index('"')
            blob = line[quote_idx:].strip('"')
            decoded = base64.b64decode(blob + "==")
            decompressed = zlib.decompress(decoded, -zlib.MAX_WBITS)
            parsed = json.loads(decompressed)
            for key, values in parsed.items():
                if isinstance(values, list):
                    collected.setdefault(key, []).extend(values)
        return collected
