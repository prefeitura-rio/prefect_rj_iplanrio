from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect import flow

from pipelines.rj_ssm__ispgeo.constants import MAX_CONCURRENT_REQUESTS, MUNICIPIO_RIO_DE_JANEIRO
from pipelines.rj_ssm__ispgeo.tasks import (
    fetch_ocorrencias_task,
    resolve_dates_task,
    upload_ocorrencias_task,
)


@flow(log_prints=True)
def rj_ssm__ispgeo(
    dataset_id: str = "brutos_ispgeo",
    table_id: str = "ocorrencias",
    dump_mode: str = "append",
    data_inicio: str | None = None,
    data_fim: str | None = None,
    todos: bool = True,
    municipio: int = MUNICIPIO_RIO_DE_JANEIRO,
    max_concurrent_requests: int = MAX_CONCURRENT_REQUESTS,
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

    upload_ocorrencias_task(
        dataframe=dataframe,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
    )