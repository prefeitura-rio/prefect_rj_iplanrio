"""The Pydantic models of the answers that a source reads from an HTTP API."""

from typing import ClassVar

from pydantic import BaseModel, ConfigDict, JsonValue, PositiveInt


class SourceOptions(BaseModel):
    """The options that a deployment gives to a source. This base has none.

    Any key that a source does not declare is an error.
    """

    model_config: ClassVar[ConfigDict] = ConfigDict(extra="forbid")


class BatchOptions(SourceOptions):
    """The options of a source that reads in chunks.

    `batch_size` is the rows of one read, and the source keeps its own size without it.
    """

    batch_size: PositiveInt | None = None


class BigQueryOptions(SourceOptions):
    """The options that a deployment gives to a BigQuery source.

    `project` is the project that runs the read.
    """

    project: str | None = None


class MongoDBOptions(BatchOptions):
    """The options that a deployment gives to a MongoDB source.

    `shape` is the path `module:name` of a PyMongoArrow `Schema`, which must have the
    field `_id`. `auth_source` is the database that holds the user, and `tls` turns
    TLS on or off.
    """

    shape: str | None = None
    auth_source: str | None = None
    tls: bool | None = None


class CouchDBOptions(BatchOptions):
    """The options that a deployment gives to a CouchDB source.

    `batch_size` is the documents of one page, `shape` the path `module:name` of a
    `Shape`, and `key_width` the digits that cut the chunks.
    """

    shape: str | None = None
    key_width: int | None = None


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
