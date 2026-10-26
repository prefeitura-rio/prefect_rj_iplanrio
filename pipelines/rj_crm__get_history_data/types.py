"""Types of rj_crm__get_history_data."""

from dataclasses import dataclass, field

from pydantic import JsonValue

type Row = dict[str, JsonValue]


@dataclass(frozen=True)
class Extension:
    """A Data Extension of SFMC: its name and its customer key."""

    name: str
    external_key: str


@dataclass(frozen=True)
class Field:
    """A column of a Data Extension: its name, type and whether it is a primary key."""

    name: str
    type: str
    is_pk: bool


@dataclass(frozen=True)
class Extraction:
    """What the extraction of a Data Extension gave.

    It has the records and columns, or the error that stopped it.
    """

    name: str
    records: list[dict[str, JsonValue]] = field(default_factory=list)
    fields: list[Field] = field(default_factory=list)
    error: str | None = None
