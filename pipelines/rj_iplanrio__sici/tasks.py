# -*- coding: utf-8 -*-
"""
Tasks específicas para a pipeline SICI.
"""
from prefect import task
from prefect.logging import get_run_logger
from zeep import Client

from iplanrio.pipelines_utils.env import getenv_or_action
from pipelines.rj_iplanrio__sici.utils import xml_to_dataframe


@task
def get_sici_api_credentials() -> dict:
    """
    Busca as credenciais da API SICI no Infisical.

    Returns:
        Dicionário com parâmetros necessários para chamada da API SOAP
    """
    logger = get_run_logger()

    try:
        consumidor = getenv_or_action(
            key="CONSUMIDOR",
        )
    except Exception as e:
        logger.error(f"Erro ao buscar credencial CONSUMIDOR do Infisical: {e}")
        raise

    try:
        chave_acesso = getenv_or_action(
            key="CHAVE_ACESSO",
        )
    except Exception as e:
        logger.error(f"Erro ao buscar credencial CHAVE_ACESSO do Infisical: {e}")
        raise

    return {
        "Codigo_UA": "",
        "Nivel": "",
        "Tipo_Arvore": "",
        "consumidor": consumidor,
        "chaveAcesso": chave_acesso,
    }


@task
def get_data_from_api_soap_sici(
    wsdl: str = "http://sici.rio.rj.gov.br/Servico/WebServiceSICI.asmx?wsdl",
    params: dict = None,
) -> str:
    """
    Busca dados da API SOAP do SICI e salva em arquivo CSV.

    Args:
        wsdl: URL do WSDL da API SOAP
        params: Dicionário com parâmetros para a chamada da API

    Returns:
        Caminho do arquivo CSV gerado
    """
    logger = get_run_logger()

    if params is None:
        params = {
            "Codigo_UA": "",
            "Nivel": "",
            "Tipo_Arvore": "",
            "consumidor": "",
            "chaveAcesso": "",
        }

    try:
        # Criar cliente SOAP
        client = Client(wsdl=wsdl)

        # Chamar o serviço
        response = client.service.Get_Arvore_UA(**params)

        # Transformar em DataFrame
        df = xml_to_dataframe(response)

        logger.info(f"Dados obtidos com sucesso da API SICI. Shape: {df.shape}")
        logger.info(f"Amostra dos dados: {df.head(5)}")

        # Salvar o DataFrame em arquivo CSV
        df.to_csv("sici_data.csv", index=False)

        # Retornar o caminho do arquivo CSV
        return "sici_data.csv"

    except Exception as e:
        logger.error(f"Erro inesperado ao buscar dados da API SICI: {e}")
        raise
