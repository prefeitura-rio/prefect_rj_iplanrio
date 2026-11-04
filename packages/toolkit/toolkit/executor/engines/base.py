"""The base of the engines: the client of one system, built from its source."""

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Mapping
from contextlib import AsyncExitStack
from importlib import import_module
from pathlib import Path
from pkgutil import iter_modules
from types import TracebackType
from typing import ClassVar, Self

from polars import DataFrame

from ...chunks import Probe, Rows
from ...types import Chunks, Columns, QueryParams
from .. import engines
from ..constants import (
    DEFAULT_BATCH_SIZE,
    INTEGER_TYPES,
    OPTIONAL_BACKENDS,
    WHOLE_DECIMAL,
)
from ..requests import UnsupportedRequestError
from ..sources.protocols import Source
from ..types import Origin


class Engine(ABC):
    """The client of one system, which does everything with it.

    A source holds the configuration, and an engine built from it opens the connection
    on `async with` and reads what `source.origin` names. The toolkit provides
    `DuckDBEngine`, `OracleEngine`, `CouchDBEngine` and `MongoDBEngine`. An engine
    handles the origins that its system can answer, with a `match` on the type, and
    raises `UnsupportedRequestError` for the others. A subclass is a dataclass whose
    first field is its source, and it sets `source_type` to the sources that it serves.
    """

    source_type: ClassVar[type[Source]]

    closer: AsyncExitStack
    token: int | None = None
    """The token that the last `probe` took on this engine, for the ledger."""

    @abstractmethod
    def __init__(self, source: Source, /) -> None:
        """Take the source. A dataclass subclass replaces this with its own."""

    @classmethod
    def from_source(cls, source: Source) -> Engine:
        """Return the unopened engine of a source.

        Raise `TypeError` if no engine serves the source.

        ```python
        async with Engine.from_source(PostgresSource(conn)) as engine:
            columns = await engine.describe()
        ```
        """
        for engine in load_engines():
            if isinstance(source, engine.source_type):
                return engine(source)

        raise TypeError(f"No engine serves {type(source).__name__}")

    async def __aenter__(self) -> Self:
        """Open the connection."""
        async with AsyncExitStack() as stack:
            await self.open(stack)
            self.closer = stack.pop_all()

        return self

    async def __aexit__(
        self,
        error_type: type[BaseException] | None,
        error: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close the connection."""
        await self.closer.__aexit__(error_type, error, traceback)

    @property
    def concurrency(self) -> int:
        """Return how many chunks a dump reads at the same time. The default is one."""
        return 1

    async def probe(self, rows: Rows, known: int | None, probe: Probe) -> bool:
        """Return `True` if the chunk must be downloaded, `False` if it did not change.

        A probe asks the system for a cheap token of the chunk, a number that changes
        whenever the rows of the chunk change, and compares it with `known`, the number
        that the ledger holds. `probe` names the kind of the token and the column to
        take the highest value of, when the engine needs one. It leaves the token in
        `token`, for the dump to record once it has downloaded the chunk. The default
        has no token and always downloads.

        An engine that overrides it keeps these rules:

        - `False` only when the token exists and equals `known`. Any doubt, any error
          and `known=None` give `True`. A probe never raises: it logs a warning and
          gives `True`, with no token.
        - The token is taken before the download and is never newer than the data
          that the download reads. A newer token would skip stale rows for good.
        - A false positive (a token that moved with no change in the rows) costs one
          download. A false negative loses data.
        - Set `token` on every call, with `None` when there is none.
        """
        self.token = None

        return True

    @abstractmethod
    async def open(self, stack: AsyncExitStack) -> None:
        """Open the connection, and push what closes it on the stack."""

    @abstractmethod
    def clone(self) -> Self:
        """Return a new unopened engine of the same source."""

    async def execute(self, sql: str, *, params: QueryParams | None = None) -> None:
        """Run one SQL statement that returns nothing.

        Raise `UnsupportedRequestError` unless the system runs SQL.
        """
        raise UnsupportedRequestError(type(self).__name__, sql)

    def query(
        self,
        sql: str,
        *,
        params: QueryParams | None = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> AsyncIterator[DataFrame]:
        """Run one SQL query, as dataframes of up to `batch_size` rows.

        A result with no rows still yields one dataframe. The dataframe has no columns
        if the client cannot tell them. Read the batches before the next call on the
        engine.
        Raise `UnsupportedRequestError` unless the system runs SQL.
        """
        raise UnsupportedRequestError(type(self).__name__, sql)

    @abstractmethod
    def read(self, *, batch_size: int = DEFAULT_BATCH_SIZE) -> AsyncIterator[DataFrame]:
        """Read the rows of the origin, as dataframes of up to `batch_size` rows."""

    @abstractmethod
    async def describe(self) -> dict[str, str]:
        """Return the name and the DuckDB type of each column of the origin."""

    @abstractmethod
    async def materialize(
        self, target: Path, rows: Rows, columns: Columns = ()
    ) -> None:
        """Write the rows of one chunk of the origin as one Parquet file.

        `columns` is the select list, and no columns means all of them. The file keeps
        the columns when there are no rows.
        """

    def check_chunks(self, chunks: Chunks, described: Mapping[str, str]) -> None:
        """Raise `ValueError` unless the chunk column fits the width of the chunks.

        A date width needs a date or a timestamp. A number of ids needs an integer, or
        a decimal with no scale, such as an Oracle `NUMBER(19)`. A text column that
        holds dates would fail later, when the system compares it to a date. An engine
        that cuts chunks its own way overrides this.
        """
        column, kind = chunks["column"], described[chunks["column"]]

        match chunks["width"]:
            case int():
                fits = (
                    kind in INTEGER_TYPES or WHOLE_DECIMAL.fullmatch(kind) is not None
                )
                wanted = "an integer"
            case _:
                fits, wanted = (
                    kind.startswith(("DATE", "TIMESTAMP")),
                    "a date or a timestamp",
                )

        if not fits:
            raise ValueError(
                f"The chunk column {column!r} is {kind}, and these chunks need {wanted} column"
            )


def require_origin(source: Source) -> Origin:
    """Return the origin of a source, or raise `ValueError` if it has none."""
    if source.origin is None:
        raise ValueError("The source has no origin to read")

    return source.origin


def load_engines() -> list[type[Engine]]:
    """Import every module of `engines/` and return the engines.

    The engine of an optional backend is left out when its extra is missing: only a
    missing module of that extra is forgiven, and any other import error raises.
    """
    for module in iter_modules(engines.__path__):
        try:
            import_module(f"{engines.__name__}.{module.name}")
        except ModuleNotFoundError as error:
            if error.name not in OPTIONAL_BACKENDS.get(module.name, ()):
                raise

    return Engine.__subclasses__()
