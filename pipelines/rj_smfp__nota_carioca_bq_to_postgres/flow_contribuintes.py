"""Flow for rj_smfp__nota_carioca_contribuintes_postgres.

Sincroniza ``public.mv_contribuintes`` (Postgres) com ``NFSE_RPE.MV_CONTRIBUINTES`` (Oracle) a cada 15 minutos, por
snapshot: lê o Oracle numa sessão ``SET TRANSACTION READ ONLY`` com o usuário somente leitura
(``DB_ORACLE_NC_LEITURA__*``), faz upsert por chave, remove o que sumiu e avança a marca d'água
``cdc_marca_dagua(fonte='mv_contribuintes')``, tudo numa transação do Postgres. Nada é escrito no Oracle. Sem as
variáveis do Oracle ou o DSN, o flow falha antes de conectar, nomeando o que falta.
"""

from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect import flow

from pipelines.rj_smfp__nota_carioca_bq_to_postgres.constants import (
    CONTRIBUINTES_ORACLE_TABLE,
    ORACLE_LEITURA_SECRET_PATH,
    SYNC_PG_DSN_ENV,
)
from pipelines.rj_smfp__nota_carioca_bq_to_postgres.tasks_contribuintes import sync_contribuintes_task


@flow(log_prints=True)
def rj_smfp__nota_carioca_contribuintes_postgres(
    oracle_table: str = CONTRIBUINTES_ORACLE_TABLE,
    infisical_secret_path: str = ORACLE_LEITURA_SECRET_PATH,
    pg_dsn_env: str = SYNC_PG_DSN_ENV,
) -> None:
    rename_current_flow_run_task(new_name="contribuintes")
    sync_contribuintes_task(
        oracle_table=oracle_table, infisical_secret_path=infisical_secret_path, pg_dsn_env=pg_dsn_env
    )
