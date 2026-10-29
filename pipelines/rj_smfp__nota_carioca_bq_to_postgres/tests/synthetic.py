"""Dados sintéticos do export do BigQuery: linhas, CSV gzip no formato do extract, manifesto e um GCS em memória.

Nenhum dado real: CPFs, nomes e chaves são inventados e fáceis de reconhecer.
"""

import gzip
import io
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import BinaryIO

BUCKET = "bucket-de-teste"
DATASET = "nota_carioca_teste"
RUN_ID = "11111111-2222-3333-4444-555555555555"
SNAPSHOT = datetime(2026, 10, 23, 9, 30, 15, 250000, tzinfo=UTC)
DETAIL = "MVT_NOTAS_NACIONAIS_EXIGIVEIS_DETALHES"
SUMMARY = "MVT_NOTAS_NACIONAIS_EXIGIVEIS"
METADATA_FIELDS = [
    ("_bigquery_particao_data", "DATE"),
    ("_bigquery_updated_at", "DATETIME"),
    ("_bigquery_uid", "STRING"),
]
DETAIL_FIELDS = [
    ("cpf_cnpj_responsavel", "STRING"),
    ("nome_contraparte", "STRING"),
    ("cpf_cnpj_contraparte", "STRING"),
    ("retencao", "INTEGER"),
    ("valor_servico", "NUMERIC"),
    ("valor_ded_red", "NUMERIC"),
    ("valor_issqn", "NUMERIC"),
    ("valor_base_calculo", "NUMERIC"),
    ("nota_fiscal", "INTEGER"),
    ("chave_acesso", "STRING"),
    ("data_validacao", "STRING"),
    ("data_competencia_municipio", "STRING"),
    ("tipo_retencao", "INTEGER"),
    ("nota_nacional", "STRING"),
    ("dps", "STRING"),
    ("opcao_simples_nacional", "INTEGER"),
    *METADATA_FIELDS,
]
SUMMARY_FIELDS = [
    ("cpf_cnpj_responsavel", "STRING"),
    ("data_competencia_municipio", "STRING"),
    ("tipo_retencao", "INTEGER"),
    ("opcao_simples_nacional", "INTEGER"),
    ("retencao", "INTEGER"),
    ("quantidade_notas", "INTEGER"),
    ("total_valor_servico", "NUMERIC"),
    ("total_valor_ded_red", "NUMERIC"),
    ("total_valor_issqn", "NUMERIC"),
    ("total_valor_base_calculo", "NUMERIC"),
    ("cnt_valor_servico", "INTEGER"),
    ("cnt_valor_ded_red", "INTEGER"),
    ("cnt_valor_issqn", "INTEGER"),
    ("cnt_valor_base_calculo", "INTEGER"),
    *METADATA_FIELDS,
]
FIELDS = {DETAIL: DETAIL_FIELDS, SUMMARY: SUMMARY_FIELDS}
Row = Mapping[str, str | int | Decimal | None]


def detail_row(number: int, **changes: str | int | Decimal | None) -> dict[str, str | int | Decimal | None]:
    """Uma nota exigível sintética; ``changes`` troca colunas (``None`` vira NULL, ``""`` vira string vazia)."""
    row: dict[str, str | int | Decimal | None] = {
        "cpf_cnpj_responsavel": "11111111111",
        "nome_contraparte": f"Contraparte {number}",
        "cpf_cnpj_contraparte": "22222222222",
        "retencao": 1,
        "valor_servico": Decimal("100.50") + number,
        "valor_ded_red": Decimal("0.10"),
        "valor_issqn": Decimal("5.02"),
        "valor_base_calculo": Decimal("100.40"),
        "nota_fiscal": 1000 + number,
        "chave_acesso": f"CHAVE{number:04d}",
        "data_validacao": "2026-03-10T14:05:09",
        "data_competencia_municipio": "2026-03-01T00:00:00",
        "tipo_retencao": 2,
        "nota_nacional": f"NN{number:04d}",
        "dps": f"DPS{number:04d}",
        "opcao_simples_nacional": 1,
        "_bigquery_particao_data": "2026-03-01",
        "_bigquery_updated_at": "2026-10-23T06:30:00",
        "_bigquery_uid": f"uid-{number}",
    }
    row.update(changes)
    return row


def summary_row(number: int, **changes: str | int | Decimal | None) -> dict[str, str | int | Decimal | None]:
    """Uma linha agregada sintética (a chave natural varia com ``number``, por competência)."""
    row: dict[str, str | int | Decimal | None] = {
        "cpf_cnpj_responsavel": "11111111111",
        "data_competencia_municipio": f"2026-{number:02d}-01T00:00:00",
        "tipo_retencao": 2,
        "opcao_simples_nacional": 1,
        "retencao": 1,
        "quantidade_notas": number,
        "total_valor_servico": Decimal("100.50") * number,
        "total_valor_ded_red": Decimal("0.10"),
        "total_valor_issqn": Decimal("5.02"),
        "total_valor_base_calculo": Decimal("100.40"),
        "cnt_valor_servico": number,
        "cnt_valor_ded_red": number,
        "cnt_valor_issqn": number,
        "cnt_valor_base_calculo": number,
        "_bigquery_particao_data": f"2026-{number:02d}-01",
        "_bigquery_updated_at": "2026-10-23T06:30:00",
        "_bigquery_uid": f"uid-{number}",
    }
    row.update(changes)
    return row


def bq_csv_line(values: Sequence[str | int | Decimal | None]) -> str:
    """Uma linha como o extract do BigQuery escreve: NULL vazio, string vazia ``""``, aspas só quando precisa."""
    cells = []
    for value in values:
        if value is None:
            cells.append("")
        elif value == "":
            cells.append('""')
        else:
            text = str(value)
            quoted = any(char in text for char in ',"\n\r')
            cells.append('"' + text.replace('"', '""') + '"' if quoted else text)
    return ",".join(cells)


def csv_gzip(table: str, rows: Sequence[Row]) -> bytes:
    """CSV gzip sem cabeçalho, com as colunas na ordem do schema do BigQuery da tabela."""
    names = [name for name, _ in FIELDS[table]]
    body = "".join(bq_csv_line([row[name] for name in names]) + "\n" for row in rows)
    return gzip.compress(body.encode("utf-8"))


@dataclass
class MemoryObjectStore:
    """GCS em memória: o adaptador injetado no lugar de ``GcsObjectStore`` (que fala com o GCS de verdade)."""

    objects: dict[tuple[str, str], bytes] = field(default_factory=dict)
    deleted: list[tuple[str, str]] = field(default_factory=list)

    def read_text(self, bucket: str, name: str) -> str:
        return self.objects[bucket, name].decode("utf-8")

    def open_binary(self, bucket: str, name: str) -> BinaryIO:
        return io.BytesIO(self.objects[bucket, name])

    def delete(self, bucket: str, names: Sequence[str]) -> None:
        for name in names:
            del self.objects[bucket, name]
            self.deleted.append((bucket, name))


@dataclass(frozen=True)
class StagedExport:
    """Um export colocado no GCS em memória."""

    store: MemoryObjectStore
    uri: str
    file_names: tuple[str, ...]
    manifest_name: str


def stage_export(  # noqa: PLR0913
    store: MemoryObjectStore,
    summary: Sequence[Row],
    detail: Sequence[Row],
    *,
    run_id: str = RUN_ID,
    manifest_rows: Mapping[str, int] | None = None,
    files_per_table: int = 2,
) -> StagedExport:
    """Grava os CSV e o manifesto no ``store``; ``manifest_rows`` força o que o manifesto diz de linhas por tabela."""
    tables, names = {}, []
    for table, rows in ((SUMMARY, summary), (DETAIL, detail)):
        parts = [rows[index::files_per_table] for index in range(files_per_table)]
        files = []
        for index, part in enumerate(parts):
            name = f"{DATASET}/{run_id}/tentativa-1/{table}/part-{index:06d}.csv.gz"
            payload = csv_gzip(table, part)
            store.objects[BUCKET, name] = payload
            files.append({"name": name, "size": len(payload)})
            names.append(name)
        count = (manifest_rows or {}).get(table, len(rows))
        fields = [{"name": name, "type": kind, "mode": "NULLABLE"} for name, kind in FIELDS[table]]
        tables[table] = {
            "files": files,
            "schema": {"fields": fields, "num_rows": count},
            "linhas": count,
            "last_modified": SNAPSHOT.isoformat(),
        }
    manifest_name = f"{DATASET}/{run_id}/manifesto.json"
    document = {"flow_run_id": run_id, "tabelas": tables, "snapshot_modified": SNAPSHOT.isoformat()}
    store.objects[BUCKET, manifest_name] = json.dumps(document).encode("utf-8")
    return StagedExport(store, f"gs://{BUCKET}/{manifest_name}", tuple(names), manifest_name)
