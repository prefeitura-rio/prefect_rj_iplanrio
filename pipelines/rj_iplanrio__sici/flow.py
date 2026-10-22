"""
Flow para extração de dados da API SOAP do SICI e upload para BigQuery.

Este flow busca dados de unidades administrativas do SICI via API SOAP,
converte os dados de XML para CSV, e faz upload para o BigQuery.
"""

from pathlib import Path

from prefect import flow
from prefect.logging import get_logger, get_run_logger

from pipelines.rj_iplanrio__sici.tasks import (
    get_data_from_api_soap_sici,
    get_sici_api_credentials,
)
from utils.tasks import publish_table_in_bq, upload_files_to_gcs
from utils.types import Cloud, Table
from utils.utils import capture_options

logger = get_logger("toolkit")


@flow(log_prints=True, flow_run_name="Dump SICI API: {dataset_id}.{table_id}")
async def rj_iplanrio__sici(
    *,
    cloud: Cloud,
    dataset_id: str,
    table_id: str,
    wsdl: str,
    dump_mode: str,
    biglake_table: bool = True,
) -> None:
    """
    Flow principal para dump de dados da API SICI para o BigQuery.

    Este flow:
    1. Renomeia o flow run com o nome da tabela
    2. Injeta credenciais do BigQuery (Base dos Dados)
    3. Busca credenciais da API SICI no Infisical
    4. Faz a chamada SOAP e obtém dados em CSV
    5. Cria tabela no BigQuery e faz upload dos dados

    Args:
        dataset_id: ID do dataset no BigQuery
        table_id: ID da tabela no BigQuery
        wsdl: URL do WSDL da API SOAP do SICI
        dump_mode: Modo de dump ("overwrite" ou "append")
        biglake_table: Se True, cria uma tabela BigLake
    """
    logger = get_run_logger()

    # Renomear flow run para facilitar identificação

    # Injetar credenciais do BigQuery

    # Buscar credenciais da API SICI
    logger.info("Buscando credenciais da API SICI...")
    credentials = get_sici_api_credentials()

    # Buscar dados da API SOAP
    logger.info("Buscando dados da API SOAP do SICI...")
    data_path = get_data_from_api_soap_sici(
        wsdl=wsdl,
        params=credentials,
    )

    # Criar tabela no BigQuery e fazer upload
    logger.info(f"Criando tabela {dataset_id}.{table_id} e fazendo upload...")
    table = Table(dataset_id=dataset_id, table_id=table_id)
    await upload_files_to_gcs(
        Path(data_path),
        table,
        bucket=cloud["bucket"],
        dump_mode=dump_mode,
        source_format="csv",
        options=capture_options(dump_mode),
    )
    await publish_table_in_bq(table, cloud)

    logger.info("Flow concluído com sucesso!")
