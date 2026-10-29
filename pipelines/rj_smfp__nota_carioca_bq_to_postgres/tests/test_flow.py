"""Entrada do flow: o que falha antes de qualquer conexão, com erro explícito."""

import pytest
from synthetic import BUCKET, DATASET, RUN_ID, MemoryObjectStore

from pipelines.rj_smfp__nota_carioca_bq_to_postgres import flow as flow_module
from pipelines.rj_smfp__nota_carioca_bq_to_postgres.utils import object_store, postgres

pytestmark = pytest.mark.usefixtures("prefect_api")
URI = f"gs://{BUCKET}/{DATASET}/{RUN_ID}/manifesto.json"


class UntouchedStore(MemoryObjectStore):
    def read_text(self, bucket: str, name: str) -> str:
        raise AssertionError(f"o GCS não podia ser lido: {bucket}/{name}")


@pytest.fixture(autouse=True)
def isolated(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(object_store, "open_store", lambda _project: UntouchedStore())
    monkeypatch.setattr(flow_module, "inject_bd_credentials_task", lambda **_kwargs: None)

    def no_connection(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("não podia conectar ao Postgres")

    monkeypatch.setattr(postgres.psycopg, "connect", no_connection)


def test_without_the_dsn_variable_the_flow_fails_before_reading_or_connecting(monkeypatch):
    monkeypatch.delenv("PG_NOTA_CARIOCA__DSN", raising=False)

    with pytest.raises(postgres.PostgresConfigError, match="PG_NOTA_CARIOCA__DSN"):
        flow_module.rj_smfp__nota_carioca_bq_to_postgres(manifesto_uri=URI, discord_notifications=False)


def test_a_uri_that_is_not_a_manifest_fails_before_reading_or_connecting(monkeypatch):
    monkeypatch.setenv("PG_NOTA_CARIOCA__DSN", "postgresql://u@h/db")

    with pytest.raises(ValueError, match="manifesto"):
        flow_module.rj_smfp__nota_carioca_bq_to_postgres(
            manifesto_uri=f"gs://{BUCKET}/{RUN_ID}.json", discord_notifications=False
        )


def test_the_dsn_variable_name_is_a_parameter(monkeypatch):
    monkeypatch.delenv("PG_OUTRO__DSN", raising=False)

    with pytest.raises(postgres.PostgresConfigError, match="PG_OUTRO__DSN"):
        flow_module.rj_smfp__nota_carioca_bq_to_postgres(
            manifesto_uri=URI, pg_dsn_env="PG_OUTRO__DSN", discord_notifications=False
        )
