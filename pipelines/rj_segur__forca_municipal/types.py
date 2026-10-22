"""Types of rj_segur__forca_municipal."""

from dataclasses import dataclass
from datetime import datetime
from typing import NotRequired, Protocol, TypedDict

from httpx2 import Response
from pydantic import JsonValue

from utils.types import Cloud, DumpMode, Table

type Row = dict[str, JsonValue]


class Page(TypedDict):
    """A page of the API: its items, and how many pages there are."""

    data: NotRequired[list[Row]]
    totalPages: NotRequired[int]


class Limits(TypedDict, total=False):
    """How much of the API a run reads at one time."""

    page_size: int
    concurrency: int
    unit_id_concurrency: int
    qmd_id_concurrency: int


class Window(TypedDict, total=False):
    """The days that `unit_positions` reads, as `YYYY-MM-DD`: yesterday if none, and one day without an end."""

    data_inicio: str
    data_fim: str


@dataclass
class Tally:
    """The ids that a read tried, and how many of them gave nothing because of an error."""

    attempted: int = 0
    errors: int = 0


class Api(Protocol):
    """The reads of the Forca Municipal API that the batches use."""

    async def get(self, path: str, params: dict[str, str | int] | None = None) -> Response:
        """Send a GET and return the response."""
        ...

    async def get_first(self, path: str, page_size: int, extra: dict[str, str] | None = None) -> tuple[int, list[Row]]:
        """Return how many pages the endpoint has, and the items of the first."""
        ...

    async def get_pages(
        self, path: str, first: int, last: int, page_size: int, extra: dict[str, str] | None = None
    ) -> list[Row]:
        """Return the items of the pages `first` to `last`."""
        ...


@dataclass(frozen=True)
class Target:
    """Where a run loads its rows: the table, the cloud, the dump mode, the start time and the date column."""

    table: Table
    cloud: Cloud
    dump_mode: DumpMode
    updated_at: datetime
    column: str


@dataclass(frozen=True)
class Endpoint:
    """A list endpoint of the API to read, and the size of its pages."""

    api: Api
    path: str
    page_size: int
