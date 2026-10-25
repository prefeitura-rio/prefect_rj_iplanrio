"""The Pydantic models of the answers that a source reads from an HTTP API."""

from pydantic import BaseModel, JsonValue


class CouchDBItem(BaseModel):
    """One document of a page, with the fields of its `_all_docs` row.

    `seq` is set by a changes read, and `last_seq` is set when the item is written.
    """

    id: str
    key: str
    value: dict[str, JsonValue] | None = None
    doc: dict[str, JsonValue] | None = None
    seq: str | None = None
    deleted: bool = False
    last_seq: str | None = None


class CouchDBAllDocs(BaseModel):
    """A page of `_all_docs`."""

    rows: list[CouchDBItem]
    update_seq: str | None = None


class CouchDBFind(BaseModel):
    """A page of `_find`."""

    docs: list[dict[str, JsonValue]]
    bookmark: str | None = None


class CouchDBChange(BaseModel):
    """One change of `_changes`: the latest revision of a document, or its deletion."""

    seq: str
    id: str
    changes: list[dict[str, JsonValue]]
    deleted: bool = False
    doc: dict[str, JsonValue] | None = None


class CouchDBChanges(BaseModel):
    """A page of `_changes`."""

    results: list[CouchDBChange]
    last_seq: str
