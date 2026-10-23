"""Task da sincronização de contribuintes: configuração, sessões e a execução única."""

from iplanrio.pipelines_utils.logging import log
from prefect import task
from prefect.cache_policies import NO_CACHE

from pipelines.rj_smfp__nota_carioca_bq_to_postgres.utils import contribuintes, oracle_leitura, postgres


@task(cache_policy=NO_CACHE)
def sync_contribuintes_task(oracle_table: str, infisical_secret_path: str, pg_dsn_env: str) -> contribuintes.SyncResult:
    """Sincroniza ``public.mv_contribuintes`` com o Oracle, numa execução só.

    Tudo o que pode estar errado na configuração é conferido antes da primeira conexão: o nome da tabela, as variáveis
    do Oracle somente leitura e o DSN do Postgres.

    :raises ValueError: Se o nome da tabela não for um identificador do Oracle.
    :raises oracle_leitura.OracleLeituraConfigError: Se faltar variável do Oracle somente leitura.
    :raises postgres.PostgresConfigError: Se faltar o DSN do Postgres.
    :raises contribuintes.EmptySnapshotError: Se o Oracle devolver zero contribuintes.
    """
    table = contribuintes.validate_table_name(oracle_table)
    config = oracle_leitura.read_config(infisical_secret_path)
    dsn = postgres.read_dsn(pg_dsn_env)
    with (
        oracle_leitura.open_read_only(config) as source,
        postgres.connect(dsn, "contribuintes_postgres") as target,
    ):
        result = contribuintes.sync_contribuintes(source, target, table)
    log(
        f"{table} → mv_contribuintes: {result.total} no Oracle (SCN {result.scn}); {result.inserted} inseridos, "
        f"{result.updated} alterados, {result.deleted} removidos"
    )
    return result
