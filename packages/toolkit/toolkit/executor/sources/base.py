"""The base of the provided sources: it holds the origin and registers by name."""

from dataclasses import field
from typing import ClassVar

from pydantic.dataclasses import dataclass

from ..constants import SOURCE_CONFIG
from ..types import Origin


@dataclass(frozen=True, slots=True, config=SOURCE_CONFIG)
class Registered:
    """The base dataclass of the provided sources.

    It holds the `origin` to read, and registers each subclass that sets a `name`, so
    `get_source` finds it. A class that does not set `name` is a base for the others.
    The keys of the options of a source are its fields, and a key that it does not
    declare is an error. A source whose engine has a probe (see `Engine.probe`) sets
    `probe_kind`, the versioned name of its token, and an empty kind means no probe.
    `range_column_needed` is true when the probe needs a column to take the highest
    value of, and then a dump probes only when it names one.
    """

    sources: ClassVar[dict[str, type[Registered]]] = {}
    name: ClassVar[str]
    probe_kind: ClassVar[str] = ""
    range_column_needed: ClassVar[bool] = False

    origin: Origin | None = field(default=None, kw_only=True)

    def __init_subclass__(cls, **kwargs: object) -> None:
        """Register the subclass if it sets a `name`."""
        super().__init_subclass__(**kwargs)

        if "name" in cls.__dict__:
            Registered.sources[cls.name] = cls
