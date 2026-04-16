# -*- coding: utf-8 -*-
import json
import os
from typing import Any, Dict

import pandas as pd
import requests
from prefect import task


def requests_get_compstat(
                        result: str,
                        endpoint : str,
                        url_base : str = "https://compstat.bugarintec.com.br/compstat/api/",
                        token_estatic : str = "compstat-rio-static-token-2026-fm-qmd") -> Dict[str, Any]:
    """
    Faz requisição GET para a API do Compstat e retorna dados específicos.

    Args:
        result (str): Chave do resultado desejado no JSON retornado.
        endpoint (str): Endpoint da API a ser acessado.
        url_base (str): URL base da API. Padrão: "https://compstat.bugarintec.com.br/compstat/api/"
        token_estatic (str): Token de autenticação estático. Padrão: "compstat-rio-static-token-2026-fm-qmd"

    Returns:
        json: Dados extraídos da chave especificada no parâmetro 'result'.
    """

    url = f"{url_base}{endpoint}"

    params = {
        "token" : token_estatic
    }

    response = requests.get(url, params=params)

    data = response.json()

    results = data.get(result)
    return results

@task
def json_compstat_to_dataframe(
    table_id: str,
    result: str,
    endpoint,
) -> pd.DataFrame:
    """
    Converte dados JSON da API Compstat em DataFrame e salva como CSV.

    Args:
        table_id (str): Identificador da tabela. Suporta 'blitz_lei_seca' e 'subareas'.
        result (str): Chave do resultado desejado no JSON retornado da API.
        endpoint: Endpoint da API Compstat a ser acessado.

    Returns:
        str: Caminho do arquivo CSV salvo.

    Raises:
        - Processa dados de forma diferente conforme o table_id:
        - 'blitz_lei_seca': Normaliza JSON diretamente e salva CSV.
        - 'subareas': Extrai dados aninhados de 'qmds' e 'elementos', explode e concatena.
    """
    requests = requests_get_compstat(
        result=result,
        endpoint=endpoint
    )
    path = os.path.join(os.getcwd(), f"pipelines/rj_ssm__compstat/{table_id}/")
    os.makedirs(path , exist_ok=True)
    output_path = os.path.join(path, "data.csv")

    # ! Tabela blitz/lei_seca
    if table_id == "blitz_lei_seca":

        df = pd.json_normalize(requests)
        df.to_csv(output_path, index=False)


    # ! Tabela subareas
    elif table_id == "subareas":
        df = pd.json_normalize(
            requests[0]['qmds']
            , sep='_'
        )

        df = df.explode('elementos')

        df_elementos = pd.json_normalize(df['elementos'], sep='_')

        df_final = pd.concat(
            [
                df.drop(columns=['elementos']).reset_index(drop=True),
                df_elementos.reset_index(drop=True)
            ],
            axis=1
        )

        df.to_csv(output_path, index=False)

    return output_path

