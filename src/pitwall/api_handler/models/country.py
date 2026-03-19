from pitwall.api_handler.models.base import F1Model


class Country(F1Model):
    key: int
    code: str
    name: str
