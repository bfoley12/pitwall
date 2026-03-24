from typing import Any, ClassVar, override

import polars as pl

from pitwall.api_handler.models.base import F1DataContainer, F1Stream


class PositionStream(F1Stream):
    SCHEMA: ClassVar[dict[str, pl.DataType]] = {
        "timestamp": pl.Duration("ms"),
        "utc": pl.Datetime(),
        "racing_number": pl.UInt8(),
        "status": pl.String(),
        # TODO: Determine best representation. Float64 is surely too large
        "x": pl.Float64(),
        "y": pl.Float64(),
        "z": pl.Float64(),
    }

    @override
    @classmethod
    def _extract_rows(
        cls, timestamp_ms: int, data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        rows = [
            {
                "utc": cls._parse_utc(data["Timestamp"]),
                "timestamp": timestamp_ms,
                "racing_number": racing_number,
                "status": entry["Status"],
                "x": entry["X"],
                "y": entry["Y"],
                "z": entry["Z"],
            }
            for racing_number, pos in data["Entries"].items()
            for entry in [pos]
        ]

        return rows


class Position(F1DataContainer):
    STREAM_FILE: ClassVar[str | None] = "Position.z.jsonStream"

    stream: PositionStream
