from typing import ClassVar, override

from pydantic import JsonValue, model_validator

from pitwall.api_handler.models.base import (
    F1Frame,
    F1KeyframeContainer,
)
from pitwall.api_handler.registry import register

from .meeting import Meeting


class SeasonKeyframe(F1Frame):
    year: int
    meetings: list[Meeting]

    @model_validator(mode="after")
    def sort_meetings(self) -> SeasonKeyframe:
        self.meetings.sort(key=lambda m: m.number)
        return self

    def get_meeting(self, query: str) -> Meeting:
        q = query.strip().casefold()
        matches: list[Meeting] = []
        for meeting in self.meetings:
            candidates = [
                meeting.name,
                meeting.location,
                meeting.circuit.short_name,
                meeting.country.name,
                meeting.folder_name,
            ]
            if any(q in c.casefold() for c in candidates):
                matches.append(meeting)

        if not matches:
            available = [f"{m.name} ({m.location})" for m in self.meetings]
            raise ValueError(
                f"No meeting matching {query!r}. Available:\n"
                + "\n".join(f"  - {a}" for a in available)
            )

        if len(matches) == 1:
            return matches[0]

        # Prefer actual race weekends over testing
        races = [m for m in matches if "testing" not in m.name.casefold()]
        if len(races) == 1:
            return races[0]

        ambiguous = [f"{m.name} ({m.location})" for m in matches]
        raise ValueError(
            f"Ambiguous match for {query!r}. Did you mean:\n"
            + "\n".join(f"  - {a}" for a in ambiguous)
        )

    @override
    def __str__(self) -> str:
        res = f"Year: {self.year}\n"
        meetings_str = "\n".join(str(meeting) for meeting in self.meetings)
        res += f"{meetings_str}"
        return res


@register
class Season(F1KeyframeContainer[SeasonKeyframe]):
    KEYFRAME_FILE: ClassVar[str | None] = "Index.json"

    keyframe: SeasonKeyframe

    @model_validator(mode="before")
    @classmethod
    def _wrap(cls, raw: dict[str, JsonValue]) -> dict[str, JsonValue]:
        if "keyframe" not in raw:
            return {"keyframe": raw}
        return raw
