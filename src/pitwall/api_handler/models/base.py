from typing import ClassVar, TypeVar

from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_pascal


class F1Model(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(
        populate_by_name=True, alias_generator=to_pascal
    )


F1ModelT = TypeVar("F1ModelT", bound=F1Model)
