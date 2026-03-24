import re
from datetime import datetime
from typing import Any, ClassVar, TypeVar

import polars as pl
from pydantic import (
    BaseModel,
    ConfigDict,
    JsonValue,
    ValidatorFunctionWrapHandler,
    model_validator,
)
from pydantic.alias_generators import to_pascal


class F1Model(BaseModel):
    """Base for all Pydantic models."""

    model_config: ClassVar[ConfigDict] = ConfigDict(
        populate_by_name=True, alias_generator=to_pascal
    )


class F1Frame(F1Model):
    @model_validator(mode="wrap")
    @classmethod
    def _unwrap_list(
        cls,
        data: list[dict[str, JsonValue]] | dict[str, JsonValue],
        handler: ValidatorFunctionWrapHandler,
    ) -> Any:
        if isinstance(data, list):
            data = data[0]
        return handler(data)


class F1Stream(F1Model):
    """Class for modeling archived jsonStream data.

    Sub-classes define SCHEMA and F1Stream will parse it on model validation.
    For complex/nested schemas, sub-classes must define their own _extract_rows method.
    F1Stream assumes PascalCase for jsonStream fields.
    """

    model_config: ClassVar[ConfigDict] = ConfigDict(
        populate_by_name=True,
        alias_generator=to_pascal,
        arbitrary_types_allowed=True,
    )

    SCHEMA: ClassVar[dict[str, pl.DataType]]

    data: pl.DataFrame

    @classmethod
    def _to_pascal(cls, name: str) -> str:
        """Convert snake_case to PascalCase."""
        return "".join(word.capitalize() for word in name.split("_"))

    @classmethod
    def _parse_timestamp(cls, ts: str) -> int:
        h, m, rest = ts.split(":")
        s, ms = rest.split(".")
        return int(h) * 3_600_000 + int(m) * 60_000 + int(s) * 1_000 + int(ms)

    @classmethod
    def _parse_utc(cls, s: str) -> datetime:
        # F1 API sends variable fractional seconds (6-7 digits); truncate to 6
        if "." in s:
            base, frac = s.rstrip("Z").split(".")
            frac = frac[:6]
            s = f"{base}.{frac}Z"
        return datetime.fromisoformat(s.replace("Z", "+00:00"))

    @classmethod
    def _parse_lap_time(cls, value: str | None) -> int | None:
        """Parse F1 lap time string like '1:26.933' to milliseconds."""
        if not value:
            return None
        match = re.fullmatch(r"(?:(\d+):)?(\d+)\.(\d+)", value)
        if not match:
            return None
        minutes = int(match.group(1) or 0)
        seconds = int(match.group(2))
        millis = int(match.group(3).ljust(3, "0")[:3])
        return minutes * 60_000 + seconds * 1_000 + millis

    @classmethod
    def _extract_rows(
        cls, timestamp_ms: int, data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Default: map SCHEMA keys to PascalCase lookups in data.

        Override for feeds with nested structure (per-car dicts, etc).
        """
        row: dict[str, Any] = {}
        for key in cls.SCHEMA:
            if key == "timestamp":
                row["timestamp"] = timestamp_ms
            else:
                row[key] = data.get(cls._to_pascal(key))
        return [row]

    @classmethod
    def _build_dataframe(cls, entries: list[dict[str, Any]]) -> pl.DataFrame:
        rows: list[dict[str, Any]] = []
        for entry in entries:
            ts_ms = cls._parse_timestamp(entry["Timestamp"])
            rows.extend(cls._extract_rows(ts_ms, entry["Data"]))
        return pl.DataFrame(rows, schema=cls.SCHEMA)

    @model_validator(mode="before")
    @classmethod
    def _from_entries(cls, raw: Any) -> dict[str, Any]:
        if isinstance(raw, list):
            return {"data": cls._build_dataframe(raw)}
        return raw


# TODO: Use Session/Index.py to dynamically set file names
class F1DataContainer(F1Model):
    """Top-level container fetched by the client."""

    model_config: ClassVar[ConfigDict] = ConfigDict(
        populate_by_name=True,
        alias_generator=to_pascal,
        arbitrary_types_allowed=True,
    )

    KEYFRAME_FILE: ClassVar[str | None] = None
    STREAM_FILE: ClassVar[str | None] = None


F1ModelT = TypeVar("F1ModelT", bound=F1Model)
F1DataContainerT = TypeVar("F1DataContainerT", bound=F1DataContainer)
