import httpx

from pitwall.api_handler.models.base import F1Model, F1ModelT
from pitwall.api_handler.models.meeting import Meeting
from pitwall.api_handler.models.season import Season
from pitwall.api_handler.models.session import SessionFeeds, SessionSubType
from pitwall.api_handler.models.timing_data import TimingData
from pitwall.api_handler.path_resolver import PathResolver


class F1Client:
    def __init__(self) -> None:
        self.http: httpx.Client = httpx.Client()

    def get_season(self, year: int) -> Season:
        return self.fetch(model=Season, year=year)

    def get_meeting(self, year: int, meeting: str) -> Meeting:
        season = self.get_season(year=year)
        return season.get_meeting(meeting)

    def get_session(self, year: int, meeting: str, session: SessionSubType) -> SessionFeeds:
        return self.fetch(model=SessionFeeds, year=year, meeting=meeting, session=session)

    def timing(
            self, year: int, meeting: str, session: SessionSubType
        ) -> TimingData:
            return self.fetch(
                model=TimingData,
                year=year,
                meeting=meeting,
                session=session,
                file="TimingDataF1.json",
            )

    def get_file(
        self, year: int, meeting: str, session: SessionSubType, file: str
    ) -> F1Model:
        return self.fetch(
            model=F1Model, year=year, meeting=meeting, session=session, file=file
        )

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
