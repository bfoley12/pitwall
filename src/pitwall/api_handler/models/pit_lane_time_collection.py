from typing import ClassVar, override

import polars as pl
from pydantic import JsonValue

from pitwall.api_handler.models.base import F1DataContainer, F1Frame, F1Stream
from pitwall.api_handler.registry import register


class PitLaneTimeCollectionStream(F1Stream):
    SCHEMA: ClassVar[dict[str, pl.DataType]] = {
        "timestamp": pl.Duration("ms"),
        "racing_number": pl.UInt8(),
        "duration": pl.Float16(),
        "lap": pl.UInt8(),
        "broadcast_deleted_at": pl.Duration("ms"),
    }

    @override
    @classmethod
    def _extract_rows(
        cls, timestamp_ms: int, data: dict[str, JsonValue]
    ) -> list[dict[str, JsonValue]]:
        rows: list[dict[str, JsonValue]] = []
        pit_times = data.get("PitTimes")
        if not isinstance(pit_times, dict):
            return rows

        for _, entry in pit_times.items():
            if isinstance(entry, list):
                for item in entry:
                    rows.append(
                        {
                            "timestamp": timestamp_ms,
                            "racing_number": item,
                            "duration": None,
                            "lap": None,
                            "broadcast_deleted_at": timestamp_ms,
                        }
                    )
            elif isinstance(entry, dict):
                rows.append(
                    {
                        "timestamp": timestamp_ms,
                        "racing_number": entry.get("RacingNumber"),
                        "duration": entry.get("Duration"),
                        "lap": entry.get("Lap"),
                        "broadcast_deleted_at": timestamp_ms
                        if entry.get("_deleted") is not None
                        else None,
                    }
                )
        return rows

    @override
    @classmethod
    def _build_dataframe(cls, entries: list[dict[str, JsonValue]]) -> pl.DataFrame:
        rows: list[dict[str, JsonValue]] = []
        for entry in entries:
            ts_ms = cls._parse_timestamp(
                entry["Timestamp"] if isinstance(entry["Timestamp"], str) else "0"
            )
            data = entry["Data"] if isinstance(entry["Data"], dict) else {}
            extracted = cls._extract_rows(ts_ms, data)
            if not extracted:
                continue

            if extracted[0].get("duration") is not None:
                rows.extend(extracted)
                continue

            for deleted_time in extracted:
                for r in reversed(rows):
                    if r["racing_number"] == deleted_time["racing_number"]:
                        r["broadcast_deleted_at"] = deleted_time["broadcast_deleted_at"]
                        break

        return pl.DataFrame(rows, schema=cls.SCHEMA)


# This is entirely unused from what I have seen, but keeping for parity
class PitLaneTimeCollectionKeyframe(F1Frame):
    pit_times: dict[str, JsonValue]


@register
class PitLaneTimeCollection(F1DataContainer[PitLaneTimeCollectionKeyframe, PitLaneTimeCollectionStream]):
    KEYFRAME_FILE: ClassVar[str | None] = "PitLaneTimeCollection.json"
    STREAM_FILE: ClassVar[str | None] = "PitLaneTimeCollection.jsonStream"

    keyframe: PitLaneTimeCollectionKeyframe
    stream: PitLaneTimeCollectionStream
