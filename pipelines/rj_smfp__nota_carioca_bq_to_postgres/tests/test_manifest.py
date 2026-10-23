"""Leitura do manifesto que o ``bq_to_oracle`` grava (contrato em ``bq_to_oracle/utils/manifest.py``)."""

import json
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError
from synthetic import BUCKET, DATASET, DETAIL, RUN_ID, SNAPSHOT, SUMMARY, MemoryObjectStore, stage_export

from pipelines.rj_smfp__nota_carioca_bq_to_postgres.utils import manifest

SUMMARY_ROWS, DETAIL_ROWS = 2, 5


def staged_text() -> str:
    staged = stage_export(
        MemoryObjectStore(), summary=[], detail=[], manifest_rows={SUMMARY: SUMMARY_ROWS, DETAIL: DETAIL_ROWS}
    )
    return staged.store.read_text(BUCKET, staged.manifest_name)


def test_the_uri_gives_bucket_object_and_dataset():
    location = manifest.parse_manifest_uri(f"gs://{BUCKET}/{DATASET}/{RUN_ID}/manifesto.json")

    assert (location.bucket, location.name, location.dataset_id) == (
        BUCKET,
        f"{DATASET}/{RUN_ID}/manifesto.json",
        DATASET,
    )


@pytest.mark.parametrize(
    "uri",
    [
        f"https://{BUCKET}/{DATASET}/{RUN_ID}/manifesto.json",
        f"gs://{BUCKET}/{DATASET}/{RUN_ID}/outro.json",
        f"gs://{BUCKET}/{RUN_ID}/manifesto.json",
        "gs:///x/y/manifesto.json",
    ],
)
def test_an_uri_that_is_not_a_manifest_of_a_flow_run_is_refused(uri: str):
    with pytest.raises(ValueError, match="manifesto"):
        manifest.parse_manifest_uri(uri)


def test_it_reads_files_schema_rows_and_the_snapshot_of_every_table():
    result = manifest.parse_manifest(staged_text())

    assert result.flow_run_id == RUN_ID
    assert result.snapshot_modified == SNAPSHOT
    detail = result.tables[DETAIL]
    assert detail.rows == DETAIL_ROWS
    assert [file.name for file in detail.files] == [
        f"{DATASET}/{RUN_ID}/tentativa-1/{DETAIL}/part-000000.csv.gz",
        f"{DATASET}/{RUN_ID}/tentativa-1/{DETAIL}/part-000001.csv.gz",
    ]
    assert detail.fields[0].name == "cpf_cnpj_responsavel"
    assert detail.fields[0].type == "STRING"
    assert result.tables[SUMMARY].rows == SUMMARY_ROWS


def test_a_snapshot_without_timezone_is_refused():
    document = json.loads(staged_text())
    document["snapshot_modified"] = datetime(2026, 10, 23, 9, 30, tzinfo=UTC).replace(tzinfo=None).isoformat()

    with pytest.raises(ValidationError):
        manifest.parse_manifest(json.dumps(document))


def test_a_negative_row_count_is_refused():
    document = json.loads(staged_text())
    document["tabelas"][DETAIL]["linhas"] = -1

    with pytest.raises(ValidationError):
        manifest.parse_manifest(json.dumps(document))
