"""The contract of a source: the configuration of the system that a dump reads."""

from dataclasses import dataclass, field
from typing import Protocol

from ..types import Origin


@dataclass(frozen=True, slots=True)
class DBConn:
    """Connection parameters of one source database."""

    hostname: str
    port: int
    database: str
    user: str
    password: str = field(repr=False)


class Source(Protocol):
    """A system that a dump reads from: only its configuration.

    A source holds the connection data, the options of the deployment and the origin to
    read. An engine built from it, `Engine.from_source(source)`, opens the connection
    and does the work. What a read needs while it runs, such as the SCN of Oracle or the
    id width of CouchDB, lives in the engine.
    """

    @property
    def origin(self) -> Origin | None:
        """Return what to read, or `None` for a source that is only connected."""
        ...
