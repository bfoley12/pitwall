import base64
import json
import zlib
from typing import Any, cast

import httpx
import polars as pl

from pitwall.api_handler.models.base import F1Model, F1ModelT
from pitwall.api_handler.models.meeting import Meeting
from pitwall.api_handler.models.season import Season
from pitwall.api_handler.models.session import SessionFeeds, SessionSubType
from pitwall.api_handler.models.timing_data import TimingDataF1
from pitwall.api_handler.path_resolver import PathResolver


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
    ) -> SessionFeeds:
        return self.fetch(
            model=SessionFeeds, year=year, meeting=meeting, session=session
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
                year=year, meeting=meeting, session=session, file="Position.z.jsonStream"
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

    def get_file(
        self, year: int, meeting: str, session: SessionSubType, file: str
    ) -> F1Model:
        return self.fetch(
            model=F1Model, year=year, meeting=meeting, session=session, file=file
        )

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
        return self._decode_compressed_stream(response.text.lstrip("\ufeff"))

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

        data = self._decode_compressed_stream(response.text.lstrip("\ufeff"))
        return model.model_validate(data)

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
