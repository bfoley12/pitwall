import httpx

from src.api_handler.models.base import F1ModelT
from src.api_handler.models.meeting import Meeting
from src.api_handler.models.season import Season
from src.api_handler.models.session import SessionSubType
from src.api_handler.path_resolver import PathResolver


class F1Client:
    def __init__(self) -> None:
        self.http: httpx.Client = httpx.Client()

    def get_season(self, year: int) -> Season:
        return self.fetch(model=Season, year=year)
    
    def get_meeting(self, year: int, meeting: str) -> Meeting:
        return self.fetch(model=Meeting, year=year, meeting=meeting)

    def fetch(
        self,
        model: type[F1ModelT],
        year: int | None = None,
        meeting: str | None = None,
        session: SessionSubType | None = None,
        file: str = "Index.json",
    ) -> F1ModelT:
        url = PathResolver(year=year, meeting=meeting, session=session, file=file).url
        response = self.http.get(url)
        _ = response.raise_for_status()
        return model.model_validate(response.json())
