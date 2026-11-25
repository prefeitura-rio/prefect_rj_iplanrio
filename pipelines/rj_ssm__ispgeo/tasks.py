"""Tasks do pipeline rj_ssm__ispgeo."""

import os

import pandas as pd
from iplanrio.pipelines_utils.env import getenv_or_action
from iplanrio.pipelines_utils.logging import log
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


@task()
def add_token_github_repo(repository_url: str) -> str:
    """
    Add the GitHub token to the repository URL.Getting it from infisical variable GITHUB_TOKEN.

    repository_url: str: The URL of the GitHub repository. This variable must be
        somethintg like https://GITHUB_TOKEN@github.com/prefeitura-rio/queries-rj-ssm-registry.git.

    Returns:
        str: The repository URL with the GitHub token added if it's a private repository.
    """
    if "GITHUB_TOKEN" in repository_url:
        log("Adding GitHub token to the repository URL from infisical variable GITHUB_TOKEN")
        token = getenv_or_action("GITHUB_TOKEN")
        repository_url = repository_url.replace("GITHUB_TOKEN", token)
    return repository_url
