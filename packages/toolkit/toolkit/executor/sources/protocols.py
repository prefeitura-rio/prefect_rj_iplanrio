"""The contract of a source: the configuration of the system that a dump reads."""

from dataclasses import dataclass, field
from typing import ClassVar, Protocol

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

    `probe_kind` names the token of the probe of its engine, and is empty when it has
    none. `range_column_needed` says whether the probe needs a column, and then a dump
    probes only when it names one.
    """

    probe_kind: ClassVar[str]
    range_column_needed: ClassVar[bool]

    @property
    def origin(self) -> Origin | None:
        """Return what to read, or `None` for a source that is only connected."""
        ...
