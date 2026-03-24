from typing import Any, ClassVar, override

import polars as pl

from pitwall.api_handler.models.base import F1DataContainer, F1Frame, F1Stream


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
        cls, timestamp_ms: int, data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for _, entry in data["PitTimes"].items():
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
            else:
                rows.append(
                    {
                        "timestamp": timestamp_ms,
                        "racing_number": entry.get("RacingNumber", None),
                        "duration": entry.get("Duration", None),
                        "lap": entry.get("Lap", None),
                        "broadcast_deleted_at": timestamp_ms
                        if entry.get("_deleted", None) is not None
                        else None,
                    }
                )
        return rows

    @classmethod
    def _build_dataframe(cls, entries: list[dict[str, Any]]) -> pl.DataFrame:
        rows: list[dict[str, Any]] = []
        for entry in entries:
            ts_ms = cls._parse_timestamp(entry["Timestamp"])
            row = cls._extract_rows(ts_ms, entry["Data"])
            # Entry was a pitstop
            if row[0].get("duration") is not None:
                rows.extend(row)
            # Entry was a broadcast delete event
            else:
                # Handle multiple deletions
                for deleted_time in row:
                    # Find the most recent pitstop for the driver being removed from broadcast
                    for r in reversed(rows):
                        if r["racing_number"] == deleted_time["racing_number"]:
                            r.update(
                                {
                                    "broadcast_deleted_at": deleted_time[
                                        "broadcast_deleted_at"
                                    ]
                                }
                            )
                            break
        return pl.DataFrame(rows, schema=cls.SCHEMA)


# This is entirely unused from what I have seen, but keeping for parity
class PitLaneTimeCollectionKeyframe(F1Frame):
    pit_times: dict[str, Any]


class PitLaneTimeCollection(F1DataContainer):
    KEYFRAME_FILE: ClassVar[str | None] = "PitLaneTimeCollection.json"
    STREAM_FILE: ClassVar[str | None] = "PitLaneTimeCollection.jsonStream"

    keyframe: PitLaneTimeCollectionKeyframe
    stream: PitLaneTimeCollectionStream
