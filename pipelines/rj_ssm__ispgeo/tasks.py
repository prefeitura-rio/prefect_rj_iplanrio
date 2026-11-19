"""Tasks do pipeline rj_ssm__ispgeo."""

import pandas as pd
from prefect import task

from pipelines.rj_ssm__ispgeo import utils
from pipelines.rj_ssm__ispgeo.constants import (
    MAX_CONCURRENT_REQUESTS,
    MUNICIPIO_RIO_DE_JANEIRO,
)


@task(retry_delay_seconds=5, retries=3)
def fetch_ocorrencias_task(
    data_inicio: str,
    data_fim: str,
    todos: bool = False,
    municipio: int = MUNICIPIO_RIO_DE_JANEIRO,
    max_concurrent_requests: int = MAX_CONCURRENT_REQUESTS,
) -> pd.DataFrame:
    """Extrai ocorrências do ISP-GEO para o período e filtro informados."""
    return utils.fetch_ocorrencias(
        data_inicio=data_inicio,
        data_fim=data_fim,
        todos=todos,
        municipio=municipio,
        max_concurrent_requests=max_concurrent_requests,
    )


@task
def resolve_dates_task(
    data_inicio: str | None,
    data_fim: str | None,
) -> dict[str, str]:
    """Resolve as datas de início e fim da janela de extração.

    Retorna dict em vez de tupla — o Prefect não suporta unpacking direto
    de tuplas retornadas por tasks. Extraia as chaves no flow:

        dates = resolve_dates_task(data_inicio, data_fim)
        data_inicio = dates["data_inicio"]
        data_fim = dates["data_fim"]
    """
    return utils.resolve_dates(data_inicio, data_fim)


@task
def upload_ocorrencias_task(
    dataframe: pd.DataFrame,
    dataset_id: str,
    table_id: str,
    dump_mode: str,
) -> None:
    """Envia o DataFrame de ocorrências para o BigQuery via GCS."""
    utils.upload_ocorrencias(
        dataframe=dataframe,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
    )
