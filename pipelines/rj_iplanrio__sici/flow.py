# -*- coding: utf-8 -*-
"""
Flow para extração de dados da API SOAP do SICI e upload para BigQuery.

Este flow busca dados de unidades administrativas do SICI via API SOAP,
converte os dados de XML para CSV, e faz upload para o BigQuery.
"""

from prefect import flow
from prefect.logging import get_run_logger

from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from iplanrio.pipelines_utils.bd import create_table_and_upload_to_gcs_task
from pipelines.rj_iplanrio__sici.tasks import (
    get_sici_api_credentials,
    get_data_from_api_soap_sici,
)


@flow(name="IPLANRIO: SICI API - Dump to BigQuery", log_prints=True)
def rj_iplanrio__sici(
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
    rename_current_flow_run_task(new_name=f"Dump SICI API: {dataset_id}.{table_id}")

    # Injetar credenciais do BigQuery
    inject_bd_credentials_task(environment="prod")

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
    create_table_and_upload_to_gcs_task(
        data_path=data_path,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
        biglake_table=biglake_table,
    )

    logger.info("Flow concluído com sucesso!")
