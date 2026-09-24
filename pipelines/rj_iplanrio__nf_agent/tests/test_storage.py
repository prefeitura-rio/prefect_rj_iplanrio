"""Tests for GCS helpers."""

from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from pipelines.rj_iplanrio__nf_agent.utils import storage


def test_parse_gcs_uri():
    assert storage.parse_gcs_uri("gs://bucket/a/b.pdf") == ("bucket", "a/b.pdf")
    assert storage.parse_gcs_uri("gs://bucket") == ("bucket", "")
    with pytest.raises(ValueError, match="gs://"):
        storage.parse_gcs_uri("bucket/a")


def test_list_pdfs_lists_direct_children_only():
    client = MagicMock()
    client.list_blobs.return_value = [
        SimpleNamespace(name="base/mes=1/"),
        SimpleNamespace(name="base/mes=1/b_doc"),
        SimpleNamespace(name="base/mes=1/a_doc.pdf"),
    ]
    with patch.object(storage.storage, "Client", return_value=client):
        refs = storage.list_pdfs("gs://bkt/base/mes=1")
    client.list_blobs.assert_called_once_with("bkt", prefix="base/mes=1/", delimiter="/")
    assert refs == [
        storage.PdfRef(name="a_doc", uri="gs://bkt/base/mes=1/a_doc.pdf"),
        storage.PdfRef(name="b_doc", uri="gs://bkt/base/mes=1/b_doc"),
    ]


def test_write_ndjson_uses_partition_and_stem():
    blob = MagicMock()
    client = MagicMock()
    client.bucket.return_value.blob.return_value = blob
    with patch.object(storage.storage, "Client", return_value=client):
        uri = storage.write_ndjson(
            "out", "staging/extracao_pagina", [{"a": 1}, {"b": "ç"}], "extracao_pagina_s1", datetime(2026, 9, 24, 12)
        )
    assert uri == "gs://out/staging/extracao_pagina/data_geracao=2026-09-24/extracao_pagina_s1.ndjson"
    payload = blob.upload_from_string.call_args.args[0].decode("utf-8")
    assert payload == '{"a": 1}\n{"b": "ç"}'
