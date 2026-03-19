from typing import ClassVar

from pydantic import ConfigDict
from pydantic.alias_generators import to_pascal

from pitwall.api_handler.models.base import F1Model


class Circuit(F1Model):
    model_config: ClassVar[ConfigDict] = ConfigDict(
        populate_by_name=True, alias_generator=to_pascal
    )
    key: int
    short_name: str
