"""The base class of the provided sources: it registers each subclass by name."""

from abc import ABC, abstractmethod
from typing import ClassVar, Self

from autoregistry import Registry
from psycopg.sql import Composable

from ...partitions import Selection
from ..engines.duckdb import Extension
from .protocols import SourceContext


class SourceBase(Registry, ABC, suffix="Source"):
    """Base of every provided source. It follows the ``Source`` protocol.

    A subclass registers under its class name without the ``Source`` suffix, in
    lower case: ``PostgresSource`` is ``"postgres"``. ``skip=True`` keeps an
    intermediate class, like ``DuckDBSource``, out of the registry.
    """

    name: ClassVar[str]
    extensions: ClassVar[tuple[Extension, ...]]

    def __init_subclass__(cls, **kwargs: object) -> None:
        super().__init_subclass__(**kwargs)
        cls.name = cls.__registry__.name

    @classmethod
    def from_context(cls, context: SourceContext) -> Self:
        """Build the source from a context. The default needs nothing from it.

        A source with required fields, like a connection, overrides this method.
        """
        return cls()

    @abstractmethod
    def window(self, query: str, selection: Selection) -> str:
        """Return the remote query restricted to the selection, in the source dialect."""

    @abstractmethod
    def setup(self) -> str | None:
        """Return the SQL to run on the engine before the scan, or ``None``."""

    @abstractmethod
    def scan(self, query: str) -> Composable:
        """Return the ``FROM`` expression that reads the query result."""
