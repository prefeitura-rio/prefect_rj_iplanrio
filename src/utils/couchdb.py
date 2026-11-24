"""The CouchDB databases of bcadastro, in the shape of `brutos_bcadastro_staging`.

Load a database from `_all_docs` with an `overwrite`, which rewrites every chunk and
drops the rows of the merges. Between two loads, merge `_changes` into the table by
`id`, with `since` set to `table`: the read resumes from the sequence that the table
holds (see `read_resume_sequence`).
"""

from typing import cast

from duckdb import connect
from pyarrow import json_, schema, string
from pydantic import JsonValue
from toolkit.delta import open_dataset, table_exists
from toolkit.executor.sources.models import CouchDBItem
from toolkit.executor.sources.shapes import Shape, convert_to_json_text

from .utils import render_query


def bcadastro_row(item: CouchDBItem) -> dict[str, JsonValue]:
    """Return one document as the destination has it, without `_airbyte_`.

    `seq` is empty when the item comes from `_all_docs`.
    """
    return {
        "id": item.id,
        "doc": convert_to_json_text(item.doc),
        "key": item.key,
        "seq": item.seq,
        "value": convert_to_json_text(item.value) if item.value is not None else None,
        "last_seq": item.last_seq,
    }


BCADASTRO = Shape(
    schema(
        [
            ("id", string()),
            ("doc", json_()),
            ("key", string()),
            ("seq", string()),
            ("value", json_()),
            ("last_seq", string()),
        ]
    ),
    bcadastro_row,
)
"""The columns of the bcadastro destination, without the `_airbyte_` ones."""


def number_of_sequence(sequence: str) -> int:
    """Return the number before the `-` of a CouchDB sequence, which grows with time."""
    return int(sequence.split("-", 1)[0])


def read_resume_sequence(uri: str) -> str:
    """Return the `_changes` sequence that the next read of a bcadastro table starts at.

    A row from `_all_docs` has no `seq`, and its `last_seq` is the sequence of the
    database when its page was read: every write after the earliest of them is in the
    changes that follow it. A row from `_changes` has a `seq`, and the latest
    `last_seq` of them is where the last read of the changes ended, after a start that
    was safe. The later of the two is returned, compared by `number_of_sequence`
    (`queries/couchdb_resume.sql` finds both).
    Raise `ValueError` if the table does not exist or holds no sequence: load it
    from `_all_docs` first.
    """
    if not table_exists(uri):
        raise ValueError(f"No table to resume the changes of: load {uri} from _all_docs first")

    rows = open_dataset(uri)

    with connect() as conn:
        conn.register("rows", rows)
        found = conn.execute(render_query(__file__, "couchdb_resume")).fetchone()

    marks = [sequence for sequence in cast("tuple[str | None, str | None]", found or (None, None)) if sequence]

    if not marks:
        raise ValueError(f"The table holds no sequence to resume from: load {uri} from _all_docs first")

    return max(marks, key=number_of_sequence)
