from pitwall.api_handler.models.base import F1Model


# TODO: Use dataclass with slots? Minor improvement - might as well not
class Country(F1Model):
    key: int
    code: str
    name: str
