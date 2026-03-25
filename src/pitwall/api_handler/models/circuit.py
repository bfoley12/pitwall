from pitwall.api_handler.models.base import F1Model


# TODO: Use dataclass with slots? Minor improvement - might as well not
class Circuit(F1Model):
    key: int
    short_name: str
