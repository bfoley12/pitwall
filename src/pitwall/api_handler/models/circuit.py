from pitwall.api_handler.models.base import F1Model


class Circuit(F1Model):
    key: int
    short_name: str
