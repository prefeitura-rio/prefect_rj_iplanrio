# -*- coding: utf-8 -*-

import os
from typing import Any, Dict, List, Optional

from prefect import flow

from pipelines.rj_crm__get_history_data.tasks import (
    authenticate_sfmc,
    fetch_data_extension_data,
    list_data_extensions_historico,
    process_historico_extraction,
)


@flow(log_prints=True)
def rj_crm__get_history_data(
    rest_uri: Optional[str] = None,
    soap_uri: Optional[str] = None,
):
    """
    Flow para extrair dados de todas as Data Extensions do SFMC com sufixo 'historico'.

    Passos:
        1. Autentica com SFMC via OAuth2 (client_credentials)
        2. Lista DEs cujo nome termina em 'historico' via SOAP API
        3. Para cada DE, extrai todos os registros via REST API com paginação
        4. Loga resumo estruturado (totais, amostras, erros)

    Args:
        rest_uri: URI REST do SFMC. Se não fornecido, usa env SFMC_REST_URI.
        soap_uri: URI SOAP do SFMC. Se não fornecido, usa env SFMC_SOAP_URI.
    """
    rest_uri = rest_uri or os.getenv("API_SFMC_REST_BASE_URL", "")
    soap_uri = soap_uri or os.getenv("API_SFMC_SOAP_BASE_URL", "")

    if not rest_uri:
        raise ValueError("API_SFMC_REST_BASE_URL não definida. Passe via parâmetro ou variável de ambiente.")
    if not soap_uri:
        raise ValueError("API_SFMC_SOAP_BASE_URL não definida. Passe via parâmetro ou variável de ambiente.")

    access_token = authenticate_sfmc()

    data_extensions = list_data_extensions_historico(
        access_token=access_token,
        soap_uri=soap_uri,
    )

    if not data_extensions:
        print("Nenhuma Data Extension com sufixo 'historico' encontrada. Flow finalizado.")
        return

    print(f"Processando {len(data_extensions)} DEs com sufixo 'historico'...")

    extraction_results: List[Dict[str, Any]] = []
    for de in data_extensions:
        try:
            rows = fetch_data_extension_data(
                access_token=access_token,
                external_key=de["external_key"],
                de_name=de["name"],
                rest_uri=rest_uri,
            )
            extraction_results.append({
                "de_name": de["name"],
                "external_key": de["external_key"],
                "status": "success",
                "total_rows": len(rows),
                "sample": rows[0] if rows else None,
            })
        except Exception as exc:
            print(f"Erro ao extrair DE '{de['name']}': {exc}")
            extraction_results.append({
                "de_name": de["name"],
                "external_key": de["external_key"],
                "status": "error",
                "total_rows": 0,
                "error": str(exc),
            })

    summary = process_historico_extraction(results=extraction_results)
    return summary
