from iplanrio.pipelines_utils.dbt import execute_dbt_task
from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect import flow
from prefect_rj_iplanrio.logging import get_logger

from pipelines.rj_ssm__ispgeo.constants import MAX_CONCURRENT_REQUESTS, MUNICIPIO_RIO_DE_JANEIRO
from pipelines.rj_ssm__ispgeo.tasks import (
    add_token_github_repo,
    fetch_ocorrencias_task,
    resolve_dates_task,
    upload_ocorrencias_task,
)

logger = get_logger(__name__)


@flow(log_prints=True)
def rj_ssm__ispgeo(  # noqa: PLR0913, PLR0917
    dataset_id: str = "brutos_ispgeo",
    table_id: str = "ocorrencias",
    dump_mode: str = "append",
    data_inicio: str | None = None,
    data_fim: str | None = None,
    todos: bool = True,
    municipio: int = MUNICIPIO_RIO_DE_JANEIRO,
    max_concurrent_requests: int = MAX_CONCURRENT_REQUESTS,
    dbt_select: str = "raw_ssm_ispgeo__ocorrencias",
    dbt_target: str = "prod",
    github_repo: str = "https://GITHUB_TOKEN@github.com/prefeitura-rio/queries-rj-ssm.git",
    dump_materialized: bool = True
) -> None:
    """Extrai ocorrências da camada de microdados do ISP-GEO e carrega no BigQuery.

    :param dataset_id: Dataset do BigQuery.
    :param table_id: Tabela do BigQuery.
    :param dump_mode: ``"append"`` para acumular dados, ``"overwrite"`` para substituir.
    :param data_inicio: Data de início (``YYYY-MM-DD``) da data de registro (``datc``).
        Default: primeiro dia do mês de 6 meses atrás.
    :param data_fim: Data de fim (``YYYY-MM-DD``) da data de registro (``datc``).
        Default: hoje.
    :param todos: Se ``True``, ignora o filtro de tipos de delito e traz tudo.
    :param municipio: Código IBGE do município do fato.
    :param max_concurrent_requests: Número máximo de requisições assíncronas simultâneas
        ao buscar páginas. Default: ``MAX_CONCURRENT_REQUESTS``.
    :param dbt_select: Seletor dbt (``--select``) dos modelos a materializar após o upload.
    :param dbt_target: Target dbt. Default: ``prod``.
    :param github_repo: Repositório dbt. ``GITHUB_TOKEN@`` na URL é substituído pelo
        segredo ``GITHUB_TOKEN`` (mesmo padrão do ``rj_ssm__run_dbt``).
    """

    rename_current_flow_run_task(new_name=f"{dataset_id}.{table_id}")
    inject_bd_credentials_task(environment="prod")

    dates = resolve_dates_task(data_inicio, data_fim)
    data_inicio = dates["data_inicio"]
    data_fim = dates["data_fim"]

    dataframe = fetch_ocorrencias_task(
        data_inicio=data_inicio,
        data_fim=data_fim,
        todos=todos,
        municipio=municipio,
        max_concurrent_requests=max_concurrent_requests,
    )

    upload_future = upload_ocorrencias_task(
        dataframe=dataframe,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
    )
    if not dump_materialized:
        logger.info("dump_materialized=False: materialização dbt ignorada para %s.%s", dataset_id, table_id)
    else:
        logger.info("Iniciando materialização dbt (select=%s, target=%s)", dbt_select, dbt_target)
        repository_url = add_token_github_repo(repository_url=github_repo)

        # Checa se a tabela de staging recebeu dados recentes (SLA de 24h definido
        # em _raw_ispgeo__sources.yml) antes de materializar o modelo.
        execute_dbt_task(
            command="source freshness",
            select="source:brutos_ispgeo_staging.ocorrencias",
            target=dbt_target,
            git_repository_path=repository_url,
            wait_for=[upload_future],
        )

        # Materializa o modelo incremental em brutos_ispgeo e roda seus testes.
        # Espera o upload terminar para ler o staging já atualizado.
        execute_dbt_task(
            command="build",
            select=dbt_select,
            target=dbt_target,
            git_repository_path=repository_url,
            wait_for=[upload_future],
        )
