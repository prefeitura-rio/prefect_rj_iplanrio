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
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from iplanrio.pipelines_utils.logging import log  # pylint: disable=E0611, E0401

DEFAULT_API_URL = "https://services.pref.rio/mcp/consulta_debitos"

# Campos de lista/dicionário que precisam ir "escapados" (aspas internas com
# barra invertida, sem aspas envolvendo o campo) em vez de JSON normal: o
# Journey Builder da SFMC substitui merge fields cru dentro do corpo JSON de
# uma HTTP activity, sem escapar aspas — se o campo já vier com aspas "de
# verdade", a substituição quebra o JSON do lado de lá. Ver memory
# project_pgm_divida_ativa_json_escape. Essas colunas também precisam ser
# passadas como `raw_columns` pro save_csv_for_sftp, pra o pandas não tentar
# re-escapar/aspar essas aspas de novo na hora de escrever o CSV.
ESCAPED_JSON_FIELDS = ["lista_cdas", "lista_guias", "dicionario_itens", "itens_informados"]

# Campos da resposta da API que viram colunas no DataFrame (nomes iguais aos
# do payload, pra rastreabilidade direta). `debitos_msg` é substituído pela
# versão formatada e legível (formatar_debitos_msg), não o JSON cru.
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

# Todas as colunas que o enricher garante no DataFrame de saída, na ordem em
# que devem entrar no de_columns do schedule (telefone/SubscriberKey já são
# tratados à parte pelo save_csv_for_sftp).
OUTPUT_FIELDS = ["cpfCnpj", *_RESPONSE_FIELDS, "itens_informados"]


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


def _escape_json_for_sfmc(value: Any) -> Optional[str]:
    """
    Serializa `value` como JSON com as aspas internas escapadas com barra
    invertida (\\"), sem aspas envolvendo o campo inteiro. Retorna None se
    `value` for None (fica vazio no CSV em vez da string "null").
    """
    if value is None:
        return None
    return json.dumps(json.dumps(value, ensure_ascii=False), ensure_ascii=False)[1:-1]


def formatar_debitos_msg(debitos: Any) -> Optional[str]:
    """
    Formata a lista de débitos (debitos_msg) em um texto legível e numerado,
    sem aparência de JSON. Pensado para ser aplicado antes de gravar o CSV
    que vai para o SFTP da Salesforce.

    Recebe `debitos` como lista de dicts (ou uma string JSON) no formato:
        [{"cda": "01/022303/2026-00", "valor": "R$4.128,56"}, ...]
        [{"guia": "2026/0055553", "data_ultimo_pagamento": "05/07/2026"}, ...]
    e devolve uma string numerada e legível, por exemplo:

        1. CDA 01/022303/2026-00 - Valor: R$4.128,56
        2. CDA 01/142572/2026-00 - Valor: R$2.186,27
        3. Guia nº 2026/0055553 - Data do Último Pagamento: 05/07/2026

    Retorna None se `debitos` for None/vazio.
    """
    if not debitos:
        return None

    if isinstance(debitos, str):
        debitos = json.loads(debitos)

    linhas = []
    for i, item in enumerate(debitos, start=1):
        if "cda" in item:
            linhas.append(f"{i}. CDA {item['cda']} - Valor: {item['valor']}")
        elif "guia" in item:
            linhas.append(
                f"{i}. Guia nº {item['guia']} - "
                f"Data do Último Pagamento: {item['data_ultimo_pagamento']}"
            )
        else:
            # fallback genérico para chaves não previstas
            partes = " - ".join(f"{k.capitalize()}: {v}" for k, v in item.items())
            linhas.append(f"{i}. {partes}")

    return "\n".join(linhas)


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

    Colunas adicionadas (sempre presentes, mesmo que vazias):
        cpfCnpj, api_resposta_sucesso, lista_cdas, lista_guias,
        dicionario_itens, total_itens_pagamento,
        mensagem_divida_contribuinte, guias_quantidade_total,
        efs_cdas_quantidade_total, total_nao_parcelado, total_parcelado,
        debitos_msg, itens_informados.

    `lista_cdas`, `lista_guias`, `dicionario_itens` e `itens_informados` vêm
    JSON-escapados pra SFMC (ver ESCAPED_JSON_FIELDS/_escape_json_for_sfmc) —
    passe essas colunas como `raw_columns` pro save_csv_for_sftp, senão o
    pandas re-escapa/aspa essas aspas de novo na hora de gravar o CSV.
    `debitos_msg` vem formatado como texto legível (formatar_debitos_msg),
    não como JSON.

    Args:
        df: DataFrame com uma coluna de CPF (default "SubscriberKey").
        params: dict com chaves opcionais:
            - cpf_column (default "SubscriberKey")
            - api_url (default DEFAULT_API_URL)

    Returns:
        DataFrame apenas com os CPFs que tiveram dívida confirmada, com as
        colunas acima anexadas.
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

        row = {cpf_column: cpf, "cpfCnpj": cpf}
        for field in _RESPONSE_FIELDS:
            value = response.get(field)
            if field == "debitos_msg":
                value = formatar_debitos_msg(value)
            elif field in ESCAPED_JSON_FIELDS:
                value = _escape_json_for_sfmc(value)
            row[field] = value

        # itens_informados: lista "1".."n" derivada das chaves de dicionario_itens
        # (n = quantidade de itens de pagamento), também escapada pra SFMC.
        dicionario_itens = response.get("dicionario_itens")
        itens: Optional[List[str]] = list(dicionario_itens.keys()) if dicionario_itens else None
        row["itens_informados"] = _escape_json_for_sfmc(itens)

        rows.append(row)

    enrichment_df = pd.DataFrame(rows, columns=[cpf_column, *OUTPUT_FIELDS])
    merged_df = df.merge(enrichment_df, on=cpf_column, how="inner")

    log(f"Débitos confirmados para {len(merged_df)} de {len(cpfs)} CPFs consultados.")
    return merged_df
