"""The toolkit origins that a dump reads, built from the parameters of a pipeline."""

from os import environ
from pathlib import Path
from re import NOFLAG
from string import Template
from typing import cast, final

from prefect.runtime import flow_run
from pydantic import JsonValue, TypeAdapter, ValidationError
from toolkit.executor.requests import Aggregate, Changes, Query, Table
from toolkit.executor.types import Origin

from .constants import (
    DOCUMENT_STORES,
    PIPELINES_FOLDER,
    PLACEHOLDER_NAME,
    STAGES_FOLDER,
    STAGES_NAME,
)
from .types import Database, Read


@final
class Placeholder(Template):
    """A text with `${NAME}` placeholders, where a name is `UPPER_SNAKE_CASE`."""

    idpattern = PLACEHOLDER_NAME
    flags = NOFLAG


def expand_environment(value: str) -> str:
    """Return the value with each `${NAME}` replaced by the environment variable `NAME`.

    Raise `ValueError` for a variable that is not set or is empty, and for a placeholder
    with a name that is not `UPPER_SNAKE_CASE`.
    """
    variables = {name: text for name, text in environ.items() if text}
    try:
        return Placeholder(value).substitute(variables)
    except KeyError as error:
        raise ValueError(f"The environment variable {error.args[0]} is not set") from None


def expand_database(database: Database) -> Database:
    """Return the database with the placeholders of its host and its name replaced.

    The `prefect.yaml` writes `host: ${TAXIRIO_MONGODB_HOST}`, and the environment of
    the run, with the Secret of the pipeline, gives the value.
    """
    return {
        **database,
        "host": expand_environment(database["host"]),
        "database": expand_environment(database["database"]),
    }


def locate_pipeline(name: str) -> Path:
    """Return the JSON file `<name>.json` of the aggregations of the running pipeline.

    The folder is `pipelines/<flow name>/pipelines`, and the flow has the name of its
    pipeline. Raise `ValueError` for a name that is not lowercase words with dashes.
    """
    if not STAGES_NAME.fullmatch(name):
        raise ValueError(f"The pipeline {name!r} is not lowercase words with dashes")

    return PIPELINES_FOLDER / cast("str", flow_run.flow_name) / STAGES_FOLDER / f"{name}.json"


def build_aggregate(db_type: str, read: Read) -> Aggregate:
    """Return the aggregation of a `read` that has a `pipeline`.

    The `pipeline` is the name of a JSON file with the list of stages, as
    `locate_pipeline` finds it. The text of the stages is compact, so the layout of the
    file does not change what a chunk hashes to.

    Raise `ValueError` for a database that is not MongoDB, for a read with no `table` or
    with a `query` or a `since`, and for a file that is not a JSON list of objects.
    """
    table = read.get("table")
    name = read.get("pipeline", "")

    if db_type != "mongodb":
        raise ValueError(f"{db_type} has no pipeline: only MongoDB aggregates")
    if table is None or "query" in read or "since" in read:
        raise ValueError("A pipeline needs a table, and takes no query or since")

    path = locate_pipeline(name)
    adapter = TypeAdapter(list[dict[str, JsonValue]])
    try:
        stages = adapter.validate_json(path.read_text(encoding="utf-8"))
    except ValidationError:
        raise ValueError(f"{path} is not a JSON list of stages, each one an object") from None

    return Aggregate(adapter.dump_json(stages).decode(), table)


def read_request(db_type: str, read: Read, batched: bool) -> Origin:
    """Return the origin that a `read` of a database names.

    A table is read in place, so the filters and the chunks go down to the database. A
    native query runs as it is, so it cannot be cut into chunks, except on Oracle,
    which applies the filters and the chunks itself. A document store (CouchDB and
    MongoDB) takes a `table` (the database or the collection), a `query` (a selector)
    and, for CouchDB, a `since` (a sequence of `_changes`), with no more than one of
    `query` and `since`. MongoDB also takes a `pipeline` of aggregation stages over a
    `table`. Raise `ValueError` for a read that does not fit.
    """
    if "pipeline" in read:
        return build_aggregate(db_type, read)

    table, query = read.get("table"), read.get("query")

    if db_type in DOCUMENT_STORES:
        return read_document_origin(db_type, table, query, read.get("since"))

    match table, query:
        case str(), None:
            return Table(table)
        case None, str():
            if batched and db_type != "oracle":
                raise ValueError("A native query is one chunk: set a table to cut it")
            return Query(query, db_type)
        case _:
            raise ValueError("Give exactly one of table and query")


def read_document_origin(db_type: str, table: str | None, query: str | None, since: str | None) -> Origin:
    """Return the origin of a CouchDB or MongoDB read.

    It is changes, a selector or a whole database.
    """
    if query is not None and since is not None:
        raise ValueError("Give one of query and since, not both")
    if since is not None and db_type != "couchdb":
        raise ValueError(f"{db_type} has no since: only CouchDB reads changes")
    if since is not None:
        return Changes(since, table)
    if query is not None:
        return Query(query, db_type, table)
    if table is not None:
        return Table(table)
    raise ValueError("Give a table, a query or a since")


def epsg_code(crs: str) -> str:
    """Return the number of an EPSG code, such as `31983` for `EPSG:31983`.

    ```python
    epsg_code("EPSG:31983")  # "31983"
    ```

    Raise `ValueError` for anything else.
    """
    code = crs.upper().removeprefix("EPSG:")
    if not code.isdigit():
        raise ValueError(f"Not an EPSG code such as EPSG:31983: {crs!r}")

    return code
