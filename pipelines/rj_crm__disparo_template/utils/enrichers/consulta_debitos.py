# -*- coding: utf-8 -*-
# flake8: noqa:E501
# pylint: disable='line-too-long'
"""
Enricher: API consulta_debitos (https://services.pref.rio/mcp/consulta_debitos).
Anexa dados de dívida ativa por CPF ao DataFrame de disparo. Chamada pública,
sem autenticação.
"""

import json
import time
from typing import Any, Dict, Optional

import pandas as pd
import requests
from iplanrio.pipelines_utils.logging import log  # pylint: disable=E0611, E0401

DEFAULT_API_URL = "https://services.pref.rio/mcp/consulta_debitos"

# Campos da resposta da API que precisam virar string (JSON) pra caber numa
# célula de CSV.
_JSON_FIELDS = ["lista_cdas", "lista_guias", "dicionario_itens", "debitos_msg"]

# Campos da resposta da API que viram colunas no DataFrame (nomes iguais aos
# do payload, pra rastreabilidade direta).
_RESPONSE_FIELDS = [
    "api_resposta_sucesso",
    "lista_cdas",
    "lista_guias",
    "dicionario_itens",
    "total_itens_pagamento",
    "mensagem_divida_contribuinte",
    "guias_quantidade_total",
    "efs_cdas_quantidade_total",
    "total_nao_parcelado",
    "total_parcelado",
    "debitos_msg",
]


def _is_sucesso(value: Any) -> bool:
    """
    Normaliza `api_resposta_sucesso` da resposta: aceita bool real ou string
    ("true"/"false"), já que APIs externas às vezes serializam bool como texto.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() == "true"
    return False


def consultar_debitos(cpf: str, api_url: str, timeout: int = 15, max_retries: int = 3) -> Optional[Dict[str, Any]]:
    """
    Consulta débitos de um CPF/CNPJ na API consulta_debitos, com retentativas
    em caso de erro de rede/timeout/5xx.

    Args:
        cpf: CPF ou CNPJ a consultar (sem autenticação, chamada pública).
        api_url: URL completa do endpoint consulta_debitos.
        timeout: Timeout em segundos por tentativa.
        max_retries: Número de tentativas antes de desistir.

    Returns:
        Dict com a resposta JSON da API, ou None se todas as tentativas falharem.
    """
    payload = {"consulta_debitos": "cpfCnpj", "cpfCnpj": cpf}

    for attempt in range(max_retries):
        try:
            response = requests.post(api_url, json=payload, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as error:
            log(f"consulta_debitos falhou para cpf={cpf} (tentativa {attempt + 1}/{max_retries}): {error}")
            if attempt < max_retries - 1:
                time.sleep(2**attempt)

    log(f"consulta_debitos esgotou as retentativas para cpf={cpf}")
    return None


def enrich_with_debitos_api(df: pd.DataFrame, params: dict = {}) -> pd.DataFrame:
    """
    Enriquece o DataFrame com dados da API consulta_debitos, um POST por CPF.

    CPFs sem resposta (falha após retentativas) ou sem dívida confirmada
    (`api_resposta_sucesso` != True) são removidos do DataFrame resultante.

    Args:
        df: DataFrame com uma coluna de CPF (default "SubscriberKey").
        params: dict com chaves opcionais:
            - cpf_column (default "SubscriberKey")
            - api_url (default DEFAULT_API_URL)

    Returns:
        DataFrame apenas com os CPFs que tiveram dívida confirmada, com as
        colunas da resposta da API anexadas.
    """
    cpf_column = params.get("cpf_column", "SubscriberKey")
    api_url = params.get("api_url", DEFAULT_API_URL)

    cpfs = df[cpf_column].dropna().unique().tolist()
    log(f"Consultando débitos para {len(cpfs)} CPFs únicos em {api_url}")

    rows = []
    for cpf in cpfs:
        response = consultar_debitos(cpf=str(cpf), api_url=api_url)
        if response is None or not _is_sucesso(response.get("api_resposta_sucesso")):
            continue

        row = {cpf_column: cpf}
        for field in _RESPONSE_FIELDS:
            value = response.get(field)
            row[field] = json.dumps(value, ensure_ascii=False) if field in _JSON_FIELDS else value
        rows.append(row)

    enrichment_df = pd.DataFrame(rows, columns=[cpf_column, *_RESPONSE_FIELDS])
    merged_df = df.merge(enrichment_df, on=cpf_column, how="inner")

    log(f"Débitos confirmados para {len(merged_df)} de {len(cpfs)} CPFs consultados.")
    return merged_df
