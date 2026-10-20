"""The CouchDB databases of bcadastro, in the shape of `brutos_bcadastro_staging`."""

from pyarrow import json_, schema, string
from pydantic import JsonValue
from toolkit.executor.sources.backends.couchdb import Shape, json_text
from toolkit.executor.sources.models import CouchDBItem


def bcadastro_row(item: CouchDBItem) -> dict[str, JsonValue]:
    """Return one document as the destination has it, without `_airbyte_`.

    `seq` is empty when the item comes from `_all_docs`.
    """
    return {
        "id": item.id,
        "doc": json_text(item.doc),
        "key": item.key,
        "seq": item.seq,
        "value": json_text(item.value) if item.value is not None else None,
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
