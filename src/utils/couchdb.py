"""The CouchDB databases of bcadastro, in the shape of `brutos_bcadastro_staging`.

Load a database once from `_all_docs` with `lookback: 0`, so a retry skips the chunks
already loaded. Then read `_changes` with `since` set to the `last_seq` of the first
chunk, which is the earliest one.
"""

from pyarrow import json_, schema, string
from pydantic import JsonValue
from toolkit.executor.sources.models import CouchDBItem
from toolkit.executor.sources.shapes import Shape, convert_to_json_text


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
