"""Manifesto do export preservado para o carregador do Postgres (``rj_smfp__nota_carioca_bq_to_postgres``).

O manifesto é o contrato entre as duas pipelines: o ``bq_to_oracle`` o grava em
``gs://<bucket>/<dataset>/<flow_run_id>/manifesto.json`` e o ``bq_to_postgres`` o lê. A leitura fica em
``bq_to_postgres/utils/manifest.py`` (cada imagem Docker copia só a sua pasta); mude os dois juntos.
"""

import json
from collections.abc import Mapping
from datetime import UTC

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.bigquery import TableSnapshot

MANIFEST_FILE_NAME = "manifesto.json"
SNAPSHOT_REFERENCE_TABLE = "MVT_NOTAS_NACIONAIS_EXIGIVEIS_DETALHES"
"""Tabela cujo comentário BQLOAD o replayer do R1 compara com ``publicacoes.snapshot_bq``."""


def manifest_object_name(dataset_id: str, flow_run_id: str) -> str:
    """Nome do objeto do manifesto no bucket, no mesmo prefixo dos arquivos exportados deste flow run.

    :param dataset_id: Dataset de origem.
    :param flow_run_id: Flow run do ``bq_to_oracle``.
    :returns: ``<dataset>/<flow_run_id>/manifesto.json``.
    """
    return f"{dataset_id}/{flow_run_id}/{MANIFEST_FILE_NAME}"


def build_manifest(flow_run_id: str, snapshot: Mapping[str, TableSnapshot]) -> dict[str, object]:
    """Monta o manifesto de uma foto exportada.

    ``snapshot_modified`` é a última alteração da tabela de referência (a mesma gravada no comentário da tabela
    BQLOAD no Oracle); sem ela na foto, vale a mais recente entre as tabelas.

    :param flow_run_id: Flow run do ``bq_to_oracle``.
    :param snapshot: Tabelas exportadas, pelo nome.
    :returns: Dicionário pronto para :func:`to_json`.
    """
    tables = {
        name: {
            "files": [{"name": exported.name, "size": exported.size} for exported in table.files],
            "schema": {"fields": table.schema["fields"], "num_rows": table.schema["num_rows"]},
            "linhas": table.schema["num_rows"],
            "last_modified": table.last_modified.astimezone(UTC).isoformat(),
        }
        for name, table in snapshot.items()
    }
    reference = snapshot.get(SNAPSHOT_REFERENCE_TABLE)
    modified = (reference or max(snapshot.values(), key=lambda table: table.last_modified)).last_modified
    return {"flow_run_id": flow_run_id, "tabelas": tables, "snapshot_modified": modified.astimezone(UTC).isoformat()}


def to_json(manifest: Mapping[str, object]) -> str:
    """Serializa o manifesto em JSON legível (UTF-8, sem escapar acentos)."""
    return json.dumps(manifest, ensure_ascii=False, indent=2)
