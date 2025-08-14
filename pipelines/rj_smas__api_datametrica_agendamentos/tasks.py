# -*- coding: utf-8 -*-
"""
Tasks migradas do Prefect 1.4 para 3.0 - SMAS API Datametrica Agendamentos
"""
# pylint: disable=invalid-name
# flake8: noqa: E501
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
import urllib3
from prefect import task  # pylint: disable=E0611, E0401

# Disable SSL warnings for internal APIs
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from iplanrio.pipelines_utils.env import getenv_or_action
from iplanrio.pipelines_utils.logging import log  # pylint: disable=E0611, E0401

from pipelines.rj_smas__api_datametrica_agendamentos.constants import (
    DatametricaConstants,
)  # pylint: disable=E0611, E0401


@task
def calculate_target_date() -> str:
    """
    Calcula a data target baseada na regra de negócio:
    - Dias normais: 2 dias à frente
    - Quinta-feira (4) e Sexta-feira (5): 4 dias à frente

    Returns:
        str: Data no formato YYYY-MM-DD
    """
    today = datetime.now()
    weekday = today.weekday()  # 0=Monday, 1=Tuesday, ..., 6=Sunday

    # Quinta-feira (3) e Sexta-feira (4) em Python weekday (0-indexed, Monday=0)
    if weekday in [3, 4]:  # Thursday and Friday
        days_ahead = 4
        log(
            "(calculate_target_date) - Quinta/sexta detectada - carregando dados para 4 dias à frente"
        )
    else:
        days_ahead = 2
        log(
            "(calculate_target_date) - Dia normal (S/T/Q) detectado - carregando dados para 2 dias à frente"
        )

    target_date = today + timedelta(days=days_ahead)
    target_date_str = target_date.strftime("%Y-%m-%d")

    log(
        f"Data target calculada: {target_date_str} (hoje: {today.strftime('%Y-%m-%d')}, {days_ahead} dias à frente)"
    )
    return target_date_str


@task
def get_datametrica_credentials() -> Dict[str, str]:
    """
    Recupera as credenciais da API da Datametrica e do proxy brasileiro usando iplanrio.

    Returns:
        Dict com 'url', 'token', 'proxy_url' e 'proxy_token'
    """

    log("Recuperando credenciais da Datametrica e do proxy brasileiro")

    # Use getenv_or_action to get secrets from environment
    url = getenv_or_action("dm_url")
    if not url.startswith("http"):
        url = f"http://{url}"

    token = getenv_or_action("dm_token")
    proxy_url = getenv_or_action("proxy_url")
    proxy_token = getenv_or_action("proxy_token")

    log("Credenciais recuperadas com sucesso")
    log(f"URL: {url}")
    log(f"Proxy URL: {proxy_url}")
    return {
        "url": url, 
        "token": token, 
        "proxy_url": proxy_url, 
        "proxy_token": proxy_token
    }


@task
def fetch_agendamentos_from_api(
    credentials: Dict[str, str], date: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Busca os agendamentos da API da Datametrica usando proxy brasileiro.

    Args:
        credentials: Dict com 'url', 'token', 'proxy_url' e 'proxy_token'
        date: Data no formato YYYY-MM-DD. Se None, usa lógica padrão (dia seguinte - DEPRECATED, usar calculate_target_date).

    Returns:
        Lista de dicionários com os dados dos agendamentos
    """
    # Build Datametrica URL
    base_url = credentials["url"].rstrip("/")
    if date is None:
        from datetime import datetime, timedelta

        date = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
    datametrica_url = f"{base_url}/{date}"
    
    # Build proxy URL
    proxy_url = f"{credentials['proxy_url'].rstrip('/')}/?url={datametrica_url}"

    log(f"Buscando agendamentos via proxy brasileiro")
    log(f"Datametrica URL: {datametrica_url}")
    log(f"Proxy URL: {proxy_url}")
    log(f"Token (primeiros 10 chars): {credentials['token'][:10]}...")

    # Headers incluindo o token do proxy e os headers originais da Datametrica
    headers = {
        "X-Proxy-Api-Token": credentials["proxy_token"],
        "Authorization": f"Bearer {credentials['token']}",
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "application/json",
        "Referer": "https://cadunico.rio/",
        "Origin": "https://cadunico.rio",
    }

    try:
        response = requests.get(proxy_url, headers=headers, timeout=30, verify=False)

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
        log(f"Erro ao buscar agendamentos via proxy: {e}")
        if hasattr(e, "response") and e.response is not None:
            log(f"Response status: {e.response.status_code}")
            log(f"Response text: {e.response.text[:500]}")
        raise
    except Exception as e:
        log(f"Erro inesperado: {e}")
        raise


@task
def transform_agendamentos_data(
    agendamentos_data: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
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
def convert_agendamentos_to_dataframe(
    agendamentos: List[Dict[str, Any]],
) -> pd.DataFrame:
    """
    Converte a lista de agendamentos para um DataFrame pandas.

    Returns:
        DataFrame com os dados dos agendamentos
    """
    log(f"Convertendo {len(agendamentos)} agendamentos para DataFrame")

    df = pd.DataFrame(agendamentos)
    log(f"DataFrame criado com {len(df)} registros e {len(df.columns)} colunas")

    return df
