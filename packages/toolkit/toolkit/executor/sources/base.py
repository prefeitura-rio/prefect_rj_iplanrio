"""The base class of the provided sources: it registers each subclass by name."""

from abc import ABC
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import ClassVar

from autoregistry import Registry

from ..engines.duckdb import Extension, load_extensions
from ..engines.protocol import Engine


class SourceBase(Registry, ABC, suffix="Source"):
    """Base of every provided source.

    A subclass registers under its class name without `Source`, in lower case.
    `skip=True` keeps an intermediate class out.
    """

    name: ClassVar[str]
    extensions: ClassVar[tuple[Extension, ...]]

    def __init_subclass__(cls, **kwargs: object) -> None:
        """Register the subclass under its class name without the `Source` suffix."""
        super().__init_subclass__(**kwargs)
        cls.name = cls.__registry__.name

    @asynccontextmanager
    async def open(self, engine: Engine) -> AsyncGenerator[Engine]:
        """Load the extensions and yield the DuckDB engine."""
        await load_extensions(engine, self.extensions)
        yield engine

    async def setup(self, client: Engine) -> None:
        """Prepare the client. The default does nothing."""
