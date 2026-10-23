"""Manifesto do export: o contrato que o carregador do Postgres lê (``bq_to_postgres/utils/manifest.py``)."""

import json
from datetime import UTC, datetime

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils import manifest
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.bigquery import ExportedFile, TableSnapshot

DETAIL = "MVT_NOTAS_NACIONAIS_EXIGIVEIS_DETALHES"
SUMMARY = "MVT_NOTAS_NACIONAIS_EXIGIVEIS"
FIELDS = [{"name": "CPF_CNPJ_RESPONSAVEL", "type": "STRING", "mode": "NULLABLE"}]
SUMMARY_ROWS = 2


def table(name: str, modified: datetime, rows: int, files: list[ExportedFile]) -> TableSnapshot:
    schema = {"fields": FIELDS, "num_rows": rows, "last_modified": modified}
    return TableSnapshot(table_id=name, schema=schema, last_modified=modified, files=files)


def test_the_manifest_sits_under_the_flow_run_prefix_of_the_exported_files():
    assert manifest.manifest_object_name("nota_carioca", "run-1") == "nota_carioca/run-1/manifesto.json"


def test_it_lists_files_schema_and_rows_of_every_table():
    # Given: duas tabelas exportadas, cada uma com a própria última alteração
    detail_files = [ExportedFile("nota_carioca/run-1/tentativa-1/DET/part-000.csv.gz", 10)]
    summary_files = [ExportedFile("nota_carioca/run-1/tentativa-1/RES/part-000.csv.gz", 7)]
    snapshot = {
        DETAIL: table(DETAIL, datetime(2026, 10, 23, 9, 0, 5, 250000, tzinfo=UTC), 5, detail_files),
        SUMMARY: table(SUMMARY, datetime(2026, 10, 23, 8, 59, 1, tzinfo=UTC), SUMMARY_ROWS, summary_files),
    }

    # When: monta o manifesto e o serializa como o flow o grava
    result = json.loads(manifest.to_json(manifest.build_manifest("run-1", snapshot)))

    # Then: cada tabela traz arquivos com tamanho, schema do BigQuery e linhas
    assert result["flow_run_id"] == "run-1"
    assert result["tabelas"][DETAIL] == {
        "files": [{"name": detail_files[0].name, "size": 10}],
        "schema": {"fields": FIELDS, "num_rows": 5},
        "linhas": 5,
        "last_modified": "2026-10-23T09:00:05.250000+00:00",
    }
    assert result["tabelas"][SUMMARY]["linhas"] == SUMMARY_ROWS
    assert result["tabelas"][SUMMARY]["files"] == [{"name": summary_files[0].name, "size": 7}]


def test_snapshot_modified_is_the_one_the_oracle_comment_of_the_detail_table_carries():
    # Given: o resumo foi alterado DEPOIS dos detalhes
    snapshot = {
        DETAIL: table(DETAIL, datetime(2026, 10, 23, 9, 0, 5, tzinfo=UTC), 5, []),
        SUMMARY: table(SUMMARY, datetime(2026, 10, 23, 9, 3, 0, tzinfo=UTC), 2, []),
    }

    # When / Then: vale a foto dos detalhes, a que o replayer do R1 compara com publicacoes.snapshot_bq
    assert manifest.build_manifest("run-1", snapshot)["snapshot_modified"] == "2026-10-23T09:00:05+00:00"


def test_snapshot_modified_falls_back_to_the_most_recent_table_without_the_detail_table():
    snapshot = {
        "A": table("A", datetime(2026, 10, 23, 9, 0, 5, tzinfo=UTC), 1, []),
        "B": table("B", datetime(2026, 10, 23, 9, 3, 0, tzinfo=UTC), 1, []),
    }

    assert manifest.build_manifest("run-1", snapshot)["snapshot_modified"] == "2026-10-23T09:03:00+00:00"
