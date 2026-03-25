from .models.base import F1Frame, F1KeyframeContainer

_REGISTRY: dict[str, type[F1KeyframeContainer[F1Frame]]] = {}


def register[T: F1KeyframeContainer](cls: type[T]) -> type[T]:
    _REGISTRY[cls.__name__] = cls
    return cls


def get(name: str) -> type[F1KeyframeContainer]:
    if name not in _REGISTRY:
        raise KeyError(
            f"No container registered as {name!r}. Available: {list(_REGISTRY)}"
        )
    return _REGISTRY[name]
