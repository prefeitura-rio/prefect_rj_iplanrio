# -*- coding: utf-8 -*-
"""
Tasks migradas do Prefect 1.4 para 3.0 - SMAS API Datametrica Agendamentos
"""
# pylint: disable=invalid-name
# flake8: noqa: E501
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
import urllib3
from prefect import task  # pylint: disable=E0611, E0401

# Disable SSL warnings for internal APIs
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from iplanrio.pipelines_utils.env import getenv_or_action
from iplanrio.pipelines_utils.logging import log  # pylint: disable=E0611, E0401

from pipelines.rj_smas__api_datametrica_agendamentos.constants import DatametricaConstants  # pylint: disable=E0611, E0401


@task
def get_datametrica_credentials() -> Dict[str, str]:
    """
    Recupera as credenciais da API da Datametrica usando iplanrio.

    Returns:
        Dict com 'url' e 'token'
    """

    log("Recuperando credenciais da Datametrica")

    url_key = DatametricaConstants.DATAMETRICA_URL.value
    token_key = DatametricaConstants.DATAMETRICA_TOKEN.value
    
    # Use getenv_or_action to get secrets from environment
    url = getenv_or_action(url_key)
    if not url.startswith("http"):
        url = f"http://{url}"

    token = getenv_or_action(token_key)

    log("Credenciais recuperadas com sucesso")
    log(f"URL: {url}")
    return {"url": url, "token": token}


@task
def fetch_agendamentos_from_api(
    credentials: Dict[str, str], date: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Busca os agendamentos da API da Datametrica.

    Args:
        credentials: Dict com 'url' e 'token' da API
        date: Data no formato YYYY-MM-DD. Se None, usa o dia seguinte.

    Returns:
        Lista de dicionários com os dados dos agendamentos
    """
    # Build URL inline to avoid import issues
    base_url = credentials["url"].rstrip("/")
    if date is None:
        from datetime import datetime, timedelta

        date = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
    url = f"{base_url}/{date}"

    log(f"Buscando agendamentos na URL: {url}")
    log(f"Token (primeiros 10 chars): {credentials['token'][:10]}...")

    headers = {
        "Authorization": f"Bearer {credentials['token']}",
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "application/json",
        "Referer": "https://cadunico.rio/",
        "Origin": "https://cadunico.rio",
    }

    try:
        response = requests.get(url, headers=headers, timeout=30, verify=False)

        # Log response details for debugging
        log(f"Status code: {response.status_code}")
        if response.status_code == 403:
            log(f"Response headers: {dict(response.headers)}")
            log(f"Response body: {response.text[:500]}")  # First 500 chars

        response.raise_for_status()

        agendamentos_data = response.json()
        log(f"Recuperados {len(agendamentos_data)} agendamentos")

        return agendamentos_data

    except requests.exceptions.RequestException as e:
        log(f"Erro ao buscar agendamentos: {e}")
        if hasattr(e, "response") and e.response is not None:
            log(f"Response status: {e.response.status_code}")
            log(f"Response text: {e.response.text[:500]}")
        raise
    except Exception as e:
        log(f"Erro inesperado: {e}")
        raise


@task
def transform_agendamentos_data(agendamentos_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Transforma e valida os dados brutos dos agendamentos.
    """
    log(f"Transformando {len(agendamentos_data)} registros")

    agendamentos = []
    for data in agendamentos_data:
        try:
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
            log(f"Erro ao processar registro: campo {e} não encontrado")
            raise
        except Exception as e:
            log(f"Erro inesperado ao processar registro: {e}")
            raise

    log(f"Transformação concluída: {len(agendamentos)} registros processados")
    return agendamentos


@task
def convert_agendamentos_to_dataframe(agendamentos: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Converte a lista de agendamentos para um DataFrame pandas.

    Returns:
        DataFrame com os dados dos agendamentos
    """
    log(f"Convertendo {len(agendamentos)} agendamentos para DataFrame")

    df = pd.DataFrame(agendamentos)
    log(f"DataFrame criado com {len(df)} registros e {len(df.columns)} colunas")

    return df