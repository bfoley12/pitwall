from typing import TypeVar

from pydantic import BaseModel


class F1Model(BaseModel):
    pass


F1ModelT = TypeVar("F1ModelT", bound=F1Model)
