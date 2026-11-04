"""Type aliases shared by the executor."""

from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING

from polars import DataType
from pyarrow import Table

from .requests import Aggregate, Changes, Files, Layer, Query, Sheet
from .requests import Table as TableOrigin
from .schema import Column
from .sources.models import CouchDBItem

if TYPE_CHECKING:
    from pymongo.collection import Collection
    from pymongoarrow.api import Schema

type Origin = TableOrigin | Query | Aggregate | Changes | Files | Layer | Sheet
"""What a source reads."""

type Expected = Mapping[str, Column | DataType | type[DataType]]
"""The columns of a read, by name: a `Column`, or a Polars type that has no nulls."""

type Page = tuple[Sequence[CouchDBItem], str | None]
"""The items of one page, and the sequence that they were read at."""

type Fetch = Callable[
    [Collection[dict[str, object]], Mapping[str, object], Schema, int], Table
]
"""Read one page of a collection: query, schema and limit, sorted by `_id`."""

type Run = Callable[
    [Collection[dict[str, object]], Sequence[Mapping[str, object]], Schema], Table
]
"""Run an aggregation of a collection: the stages and the schema of the result."""
