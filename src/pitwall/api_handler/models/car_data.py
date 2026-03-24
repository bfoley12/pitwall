from typing import Any, ClassVar, override

import polars as pl

from pitwall.api_handler.models.base import F1DataContainer, F1Stream

# TODO: Decode Keyframe too (not actually informative, but just for completeness)
# TODO: If F1 releases 2026 data (active aero and battery) reassess this labeling
# - Might need to version if channels change
CHANNEL_MAP: dict[str, str] = {
    "0": "rpm",
    "2": "speed",
    "3": "rpm",
    "4": "throttle",
    "5": "brake",
    "45": "drs",
}


class CarDataStream(F1Stream):
    SCHEMA: ClassVar[dict[str, pl.DataType]] = {
        "timestamp": pl.Duration("ms"),
        "utc": pl.Datetime(),
        "racing_number": pl.UInt8(),
        "rpm": pl.UInt16(),
        "speed": pl.UInt16(),
        "gear": pl.UInt8(),
        "throttle": pl.Float16(),
        # Only values from 2023 Abu Dhabi: [0, 100, 104]
        "brake": pl.UInt8(),
        # Odd values from 2023 Abu Dhabi (only one tested so far) - [0, 1, 2, 3, 8, 10, 12, 14]
        "drs": pl.UInt8(),
    }

    @override
    @classmethod
    def _extract_rows(
        cls, timestamp_ms: int, data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        rows = [
            {
                "timestamp": timestamp_ms,
                "utc": cls._parse_utc(data["Utc"]),
                "racing_number": racing_num,
                "rpm": ch.get("0", 0),
                "speed": ch.get("2", 0),
                "gear": ch.get("3", 0),
                "throttle": ch.get("4", 0),
                "brake": ch.get("5", 0),
                "drs": ch.get("45"),
            }
            for racing_num, car in data["Cars"].items()
            for ch in [car["Channels"]]
        ]

        return rows


class CarData(F1DataContainer):
    STREAM_FILE: ClassVar[str | None] = "CarData.z.jsonStream"

    stream: CarDataStream
