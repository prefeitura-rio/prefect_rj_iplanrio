from pathlib import Path

import pytest
import yaml

from pipelines.rj_smfp__nota_carioca_oracle_to_bq.constants import (
    CHECKSUM_COLUMNS,
    CLUSTER_KEYS,
    DEFAULT_TABLES,
    SOURCE_SCHEMAS,
)
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils import bigquery
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.plan import default_layout, resolve_source_schema

PIPELINE_DIR = Path(__file__).resolve().parents[1]
ORIGINAL_TABLES = ("DPS", "NOTAS_NACIONAIS", "PESSOAS_NACIONAIS")
# tabela de destino -> dono no Oracle (o nome no BigQuery é o da tabela, como nas três originais)
NEW_REPLICAS = {
    "MV_CONTRIBUINTES": "NFSE_RPE",
    "MV_CONTRIBUINTES_HISTORICOS": "NFSE_RPE",
    "DESIFINSTITUICOES": "NFSE_OWNER",
    "CONFIGURACOES": "NFSE_OWNER",
    "EVENTOS_DFEN": "DFEN",
}


def deployment_dataset(environment: str) -> str:
    config = yaml.safe_load((PIPELINE_DIR / "prefect.yaml").read_text(encoding="utf-8"))
    (deployment,) = [d for d in config["deployments"] if d["name"].endswith(f"--{environment}")]
    return str(deployment["parameters"]["dataset_id"])


def test_default_tables_keep_the_original_three_first_and_add_the_five_replicas() -> None:
    assert DEFAULT_TABLES[:3] == ORIGINAL_TABLES
    assert set(DEFAULT_TABLES[3:]) == set(NEW_REPLICAS)
    assert len(DEFAULT_TABLES) == len(set(DEFAULT_TABLES))


@pytest.mark.parametrize(("table", "owner"), NEW_REPLICAS.items())
def test_each_replica_reads_its_own_oracle_owner_and_lands_in_brutos_nota_fiscal(table: str, owner: str) -> None:
    # Given a run with no source_schema override, and one forcing the schema of the original tables
    # When the owner is resolved
    # Then the replica keeps its owner either way, and the prod deployment writes to brutos_nota_fiscal
    assert resolve_source_schema(table, None, "DFEN") == owner
    assert resolve_source_schema(table, "OUTRO", "DFEN") == owner
    assert deployment_dataset("prod") == "brutos_nota_fiscal"
    assert deployment_dataset("staging") == "brutos_nota_fiscal_staging"


@pytest.mark.parametrize("table", ORIGINAL_TABLES)
def test_original_tables_keep_resolving_the_owner_from_the_override_or_the_secret(table: str) -> None:
    assert table not in SOURCE_SCHEMAS
    assert resolve_source_schema(table, None, "DFEN") == "DFEN"
    assert resolve_source_schema(table, "OUTRO", "DFEN") == "OUTRO"


@pytest.mark.parametrize("table", NEW_REPLICAS)
def test_each_replica_has_cluster_key_and_checksum_definition(table: str) -> None:
    assert table in CLUSTER_KEYS
    assert default_layout(table).clustering == (CLUSTER_KEYS[table], "_airbyte_extracted_at")
    assert table in CHECKSUM_COLUMNS


def test_config_table_without_number_columns_is_validated_by_row_count_alone(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given a table with no checksum columns and a BigQuery client that fails on any query
    def fail(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("não deve consultar o BigQuery")

    monkeypatch.setattr(bigquery.bigquery, "Client", fail)

    # When the checksum is read, then nothing is queried
    assert CHECKSUM_COLUMNS["CONFIGURACOES"] == ()
    assert bigquery.read_checksums("p", "d", "CONFIGURACOES__oracle_to_bq_tmp", ()) == {}
