from typing import ClassVar

from pydantic import ConfigDict
from pydantic.alias_generators import to_pascal

from pitwall.api_handler.models.base import F1Model


class Circuit(F1Model):
    key: int
    short_name: str
