"""The closed set of origins that a source names and an engine reads.

An origin tells what to read. An engine handles the origins that its system can answer.
"""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Table:
    """A table of the system: `schema.name`, or the collection or database of NoSQL."""

    name: str


@dataclass(frozen=True, slots=True)
class Query:
    """A native query: SQL text, or a selector for a document store.

    `dialect` names the SQL source that DuckDB reads it through. `table` names the
    collection or the database that a selector runs on.
    """

    text: str
    dialect: str = ""
    table: str | None = None


@dataclass(frozen=True, slots=True)
class Aggregate:
    """An aggregation of a MongoDB collection: its stages as JSON text, and the name.

    The stages run after a `$match` that holds the filters and the chunk, so the chunk
    field is the one of the stored documents.
    """

    stages: str
    table: str


@dataclass(frozen=True, slots=True)
class Changes:
    """The changes of a CouchDB database after a sequence.

    `table` names the database, and `None` means the one of the connection.
    """

    since: str
    table: str | None = None


@dataclass(frozen=True, slots=True)
class Files:
    """Parquet, CSV or JSON files, by path or by glob."""

    glob: str
    format: str


@dataclass(frozen=True, slots=True)
class Layer:
    """An ArcGIS REST layer: its URL without `/query`, a filter and a system."""

    url: str
    where: str = "1=1"
    srid: str | None = None


@dataclass(frozen=True, slots=True)
class Sheet:
    """A Google Sheet by its id, and one of its sheets (the first without a name)."""

    id: str
    name: str | None = None


class UnsupportedRequestError(TypeError):
    """An engine got an origin or a statement that it does not handle."""

    def __init__(self, engine: str, request: object) -> None:
        """Name the engine and the request."""
        super().__init__(f"{engine} does not handle {type(request).__name__}")
