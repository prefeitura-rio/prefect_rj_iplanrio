"""
Tasks migradas do Prefect 1.4 para 3.0 - SMAS API Datametrica Agendamentos
"""

# pylint: disable=invalid-name
# flake8: noqa: E501
import json
from datetime import datetime, timedelta
from typing import Any, Dict, List
from zoneinfo import ZoneInfo

import pandas as pd
import requests
import urllib3
from prefect import task  # pylint: disable=E0611, E0401
from prefect.logging import get_logger

from utils.utils import read_env

logger = get_logger("toolkit")

# Disable SSL warnings for internal APIs
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


@task
def calculate_target_date() -> str | None:
    """
    Calcula a data target baseada na regra de negócio:
    - Sábado (5) ou Domingo (6): retorna None (flow deve ser encerrado)
    - Dias normais: 2 dias à frente
    - Quinta-feira (3) e Sexta-feira (4): 4 dias à frente

    Returns:
        str: Data no formato YYYY-MM-DD, ou None se for sábado ou domingo
    """
    today = datetime.now(ZoneInfo("America/Sao_Paulo"))
    weekday = today.weekday()  # 0=Monday, 1=Tuesday, ..., 6=Sunday

    if weekday in [5, 6]:  # Saturday or Sunday
        logger.info(f"(calculate_target_date) - Fim de semana detectado (weekday={weekday}) - encerrando flow")
        return None

    # Quinta-feira (3) e Sexta-feira (4) em Python weekday (0-indexed, Monday=0)
    if weekday in [3, 4]:  # Thursday and Friday
        days_ahead = 4
        logger.info("(calculate_target_date) - Quinta/sexta detectada - carregando dados para 4 dias à frente")
    else:
        days_ahead = 2
        logger.info("(calculate_target_date) - Dia normal (S/T/Q) detectado - carregando dados para 2 dias à frente")

    target_date = today + timedelta(days=days_ahead)
    target_date_str = target_date.strftime("%Y-%m-%d")

    logger.info(f"Data target calculada: {target_date_str} (hoje: {today.strftime('%Y-%m-%d')}, {days_ahead} dias à frente)")
    return target_date_str


@task
def get_datametrica_credentials(
    infisical_secret_path: str | None = None,
) -> Dict[str, str]:
    """
    Recupera as credenciais da API da Datametrica e do proxy brasileiro usando toolkit.

    Args:
        infisical_secret_path: Caminho dos secrets no Infisical (usado pelo sidecar)

    Returns:
        Dict com 'url', 'token', 'proxy_url' e 'proxy_token'
    """

    logger.info("Recuperando credenciais da Datametrica e do proxy brasileiro")

    # Use getenv_or_action to get secrets from environment
    url = read_env("dm_url")
    if not url.startswith("http"):
        url = f"http://{url}"

    token = read_env("dm_token")
    proxy_url = read_env("proxy_url")
    proxy_token = read_env("proxy_token")

    logger.info("Credenciais recuperadas com sucesso")

    return {
        "url": url,
        "token": token,
        "proxy_url": proxy_url,
        "proxy_token": proxy_token,
    }


@task
def fetch_agendamentos_from_api(credentials: Dict[str, str], date: str) -> List[Dict[str, Any]]:
    """
    Busca os agendamentos da API da Datametrica usando proxy brasileiro.

    Args:
        credentials: Dict com 'url', 'token', 'proxy_url' e 'proxy_token'
        date: Data no formato YYYY-MM-DD. Sempre obrigatória — use calculate_target_date() para obter a data correta.

    Returns:
        Lista de dicionários com os dados dos agendamentos
    """
    # Build Datametrica URL
    base_url = credentials["url"].rstrip("/")
    datametrica_url = f"{base_url}/{date}"

    # Build proxy URL
    proxy_url = f"{credentials['proxy_url'].rstrip('/')}/?url={datametrica_url}"

    logger.info("Buscando agendamentos via proxy brasileiro")

    # Headers incluindo o token do proxy e os headers originais da Datametrica
    headers = {
        "X-Proxy-Api-Token": credentials["proxy_token"],
        "Authorization": f"Bearer {credentials['token']}",
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "application/json",
    }

    try:
        response = requests.get(proxy_url, headers=headers, timeout=30)

        # Log response details for debugging
        logger.info(f"Status code: {response.status_code}")
        if response.status_code == 403:
            logger.info(f"Response headers: {dict(response.headers)}")
            logger.info(f"Response body: {response.text[:500]}")  # First 500 chars

        response.raise_for_status()

        if not response.text or not response.text.strip():
            logger.info(f"Aviso: API retornou body vazio. Retornando lista vazia.")
            return []

        agendamentos_data = response.json()
        logger.info(f"Recuperados {len(agendamentos_data)} agendamentos")

        return agendamentos_data

    except requests.exceptions.RequestException as e:
        logger.info(f"Erro ao buscar agendamentos via proxy: {e}")
        if hasattr(e, "response") and e.response is not None:
            logger.info(f"Response status: {e.response.status_code}")
            logger.info(f"Response text: {e.response.text[:500]}")
        raise
    except Exception as e:
        logger.info(f"Erro inesperado: {e}")
        raise


@task
def transform_agendamentos_data(
    agendamentos_data: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Transforma e valida os dados brutos dos agendamentos.
    """
    logger.info(f"Transformando {len(agendamentos_data)} registros")

    agendamentos = []
    for data in agendamentos_data:
        if len(agendamentos_data) < 10:
            logger.info(f"Poucos registros recebidos como string: {data}")
        try:
            if isinstance(data, str):
                if not data.strip():
                    logger.info("Registro ignorado: string vazia recebida da API")
                    continue
                data = json.loads(data)
            agendamento = {
                "id": data["id"],
                "id_capacidade": data["id_capacidade"],
                "nome_completo": data["nome_completo"],
                "primeiro_nome": data["primeiro_nome"],
                "cpf": data["cpf"],
                "telefone": data["telefone"],
                "tipo": data["tipo"],
                "data_hora": data["data_hora"],
                "unidade_nome": data["nome"],
                "unidade_endereco": data["endereco"],
                "unidade_bairro": data["bairro"],
            }
            agendamentos.append(agendamento)
        except KeyError as e:
            logger.info(f"Erro ao processar registro: campo {e} não encontrado")
            raise
        except Exception as e:
            logger.info(f"Erro inesperado ao processar registro: {e}")
            raise

    logger.info(f"Transformação concluída: {len(agendamentos)} registros processados")
    return agendamentos


@task
def get_bigquery_config(
    infisical_secret_path: str | None = None,
) -> Dict[str, str]:
    """
    Recupera dataset_id e table_id do BigQuery usando toolkit.

    Args:
        infisical_secret_path: Caminho dos secrets no Infisical (usado pelo sidecar)

    Returns:
        Dict com 'dataset_id' e 'table_id'
    """
    logger.info("Recuperando configuração do BigQuery do Infisical")

    # Use getenv_or_action to get secrets from environment
    dataset_id = read_env("dataset_id")
    table_id = read_env("table_id")

    logger.info("Configuração do BigQuery recuperada com sucesso")

    return {
        "dataset_id": dataset_id,
        "table_id": table_id,
    }


@task
def convert_agendamentos_to_dataframe(
    agendamentos: List[Dict[str, Any]],
) -> pd.DataFrame:
    """
    Converte a lista de agendamentos para um DataFrame pandas.

    Returns:
        DataFrame com os dados dos agendamentos
    """
    logger.info(f"Convertendo {len(agendamentos)} agendamentos para DataFrame")

    df = pd.DataFrame(agendamentos)
    logger.info(f"DataFrame criado com {len(df)} registros e {len(df.columns)} colunas")

    return df

