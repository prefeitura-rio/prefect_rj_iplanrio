"""The shape of a CouchDB row: its columns, and how a document becomes one."""

from collections.abc import Callable
from dataclasses import dataclass
from json import dumps

from pyarrow import Schema
from pydantic import JsonValue

from .models import CouchDBItem


def convert_to_json_text(value: JsonValue) -> str:
    """Return a value as compact JSON text."""
    return dumps(value, ensure_ascii=False, separators=(",", ":"))


def build_document_row(item: CouchDBItem) -> dict[str, JsonValue]:
    """Return the id and the document of one item."""
    return {"id": item.id, "doc": convert_to_json_text(item.doc)}


@dataclass(frozen=True, slots=True)
class Shape:
    """The columns of the stage file, and how one item of a page becomes a row.

    A database with other columns takes its own shape, with no new source class.
    """

    columns: Schema
    row: Callable[[CouchDBItem], dict[str, JsonValue]]
