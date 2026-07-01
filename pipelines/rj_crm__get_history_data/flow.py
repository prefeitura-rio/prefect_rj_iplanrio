# -*- coding: utf-8 -*-

import os
from typing import Any, Dict, List, Optional

from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from prefect import flow

from pipelines.rj_crm__get_history_data.constants import GetHistoryDataConstants
from pipelines.rj_crm__get_history_data.tasks import (
    authenticate_sfmc,
    build_historico_dataframe,
    fetch_data_extension_data,
    fetch_de_field_types,
    filter_recent_and_partition,
    list_data_extensions_historico,
    load_recent_data_to_tmp_table,
    merge_tmp_into_historico,
    process_historico_extraction,
)


@flow(log_prints=True)
def rj_crm__get_history_data(
    rest_uri: Optional[str] = None,
    soap_uri: Optional[str] = None,
    window_days: Optional[int] = None,
):
    """
    Flow para extrair dados de todas as Data Extensions do SFMC com sufixo 'historico'
    e publicar o resultado consolidado no BigQuery (rj-crm-registry.brutos_salesforce_staging).

    Passos:
        1. Autentica com SFMC via OAuth2 (client_credentials)
        2. Lista DEs cujo nome termina em 'historico' via SOAP API
        3. Para cada DE, extrai todos os registros via REST API com paginação
        4. Loga resumo estruturado (totais, amostras, erros)
        5. Monta o DataFrame consolidado (de_nome, telefone, id_de, entrada_data, dados_json)
        6. Filtra apenas registros com entrada_data dentro dos últimos `window_days`
           dias (registros sem entrada_data identificável são sempre mantidos)
        7. Trunca e recarrega a tabela temporária historico_sfmc_tmp com esses dados
        8. Faz o MERGE da tabela temporária na tabela final historico (upsert por
           de_nome + telefone + entrada_data) — nunca apaga dados da tabela final

    Args:
        rest_uri: URI REST do SFMC. Se não fornecido, usa env API_SFMC_REST_BASE_URL.
        soap_uri: URI SOAP do SFMC. Se não fornecido, usa env API_SFMC_SOAP_BASE_URL.
        window_days: Quantos dias de entrada_data considerar a cada execução.
            Se não fornecido, usa GetHistoryDataConstants.WINDOW_DAYS.
    """
    rest_uri = rest_uri or os.getenv("API_SFMC_REST_BASE_URL", "")
    soap_uri = soap_uri or os.getenv("API_SFMC_SOAP_BASE_URL", "")
    window_days = window_days or GetHistoryDataConstants.WINDOW_DAYS.value

    if not rest_uri:
        raise ValueError("API_SFMC_REST_BASE_URL não definida. Passe via parâmetro ou variável de ambiente.")
    if not soap_uri:
        raise ValueError("API_SFMC_SOAP_BASE_URL não definida. Passe via parâmetro ou variável de ambiente.")

    project_id = GetHistoryDataConstants.GCP_SFMC_DESTINATION_PROJECT_ID.value
    dataset_id = GetHistoryDataConstants.GCP_SFMC_DESTINATION_DATASET_ID.value
    tmp_table_id = GetHistoryDataConstants.GCP_SFMC_TMP_TABLE_ID.value
    final_table_id = GetHistoryDataConstants.GCP_SFMC_FINAL_TABLE_ID.value

    inject_bd_credentials_task(environment="prod")

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
            field_types = fetch_de_field_types(
                access_token=access_token,
                customer_key=de["external_key"],
                de_name=de["name"],
                soap_uri=soap_uri,
            )
            extraction_results.append({
                "de_name": de["name"],
                "external_key": de["external_key"],
                "status": "success",
                "total_rows": len(rows),
                "dados": rows,
                "tipos": field_types,
            })
        except Exception as exc:
            print(f"Erro ao extrair DE '{de['name']}': {exc}")
            extraction_results.append({
                "de_name": de["name"],
                "external_key": de["external_key"],
                "status": "error",
                "total_rows": 0,
                "dados": [],
                "tipos": [],
                "error": str(exc),
            })

    process_historico_extraction(results=extraction_results)
    df_historico = build_historico_dataframe(results=extraction_results)
    df_recent = filter_recent_and_partition(df=df_historico, window_days=window_days)

    client = load_recent_data_to_tmp_table(
        df=df_recent,
        project_id=project_id,
        dataset_id=dataset_id,
        tmp_table_id=tmp_table_id,
        final_table_id=final_table_id,
    )
    merge_tmp_into_historico(
        client=client,
        project_id=project_id,
        dataset_id=dataset_id,
        tmp_table_id=tmp_table_id,
        final_table_id=final_table_id,
    )
