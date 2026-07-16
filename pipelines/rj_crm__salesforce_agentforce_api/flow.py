# -*- coding: utf-8 -*-
"""
Flow genérico para ingestão de dados via Salesforce CRM Bulk API 2.0.

Diferente da pipeline rj_crm__salesforce_api (que acessa a SFMC REST API),
esta pipeline acessa a org do Salesforce CRM onde ficam os dados do Agentforce:
sessões de agente, mensagens, conversas e demais objetos CRM.

O método de ingestão usa a Bulk API 2.0, que é assíncrona:
  1. Cria job SOQL
  2. Aguarda conclusão
  3. Baixa CSV e carrega no BigQuery

Os dados são salvos com duas colunas:
  - data          : JSON string com o conteúdo completo do registro
  - data_particao : data de execução (usada para particionamento)

Credenciais necessárias no Infisical (path: configurável por parâmetro):
  - SF_CRM_CLIENT_ID      : Client ID da Connected App no Salesforce CRM
  - SF_CRM_CLIENT_SECRET  : Client Secret da Connected App no Salesforce CRM
  - SF_CRM_USERNAME       : Username do usuário de integração
  - SF_CRM_PASSWORD       : Senha + Security Token (ex: SenhaXXXTOKENYYY)
  - SF_CRM_LOGIN_URL      : https://login.salesforce.com (ou https://test.salesforce.com para sandbox)

Objetos Agentforce disponíveis via SOQL (dependem da licença e configuração da org):
  - BotSession         : sessões de conversa com o agente
  - BotMessage         : mensagens individuais trocadas na sessão
  - ConversationEntry  : entradas detalhadas da conversa
  - MessagingSession   : sessões de mensageria (WhatsApp, SMS, etc.)
  - AgentWork          : trabalho roteado pelo Omni-Channel
  - Case               : casos criados/resolvidos pelo agente
"""

import os

from iplanrio.pipelines_utils.bd import create_table_and_upload_to_gcs_task
from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect import flow

from pipelines.rj_crm__salesforce_agentforce_api.constants import AgentforceConstants
from pipelines.rj_crm__salesforce_agentforce_api.tasks import (
    build_dataframe,
    fetch_crm_bulk_query,
    list_available_objects,
)
from pipelines.rj_crm__disparo_template.utils.tasks import create_date_partitions


@flow(log_prints=True)
def rj_crm__salesforce_agentforce_api(
    soql: str,
    table_id: str,
    dataset_id: str | None = None,
    dump_mode: str | None = None,
    api_version: str = "v59.0",
    poll_interval_seconds: int = 5,
    max_wait_seconds: int = 600,
    infisical_secret_path: str | None = None,
    discover_objects: bool = False,
):
    """
    Flow genérico para ingestão de objetos do Salesforce CRM via Bulk API 2.0.

    Executa uma query SOQL assíncrona, aguarda conclusão, baixa os resultados
    em CSV e carrega no BigQuery particionado por data de execução.

    Args:
        soql: Query SOQL a executar.
              Pode usar filtros de data como YESTERDAY, LAST_N_DAYS:N, ou
              ranges explícitos (ex: WHERE StartTime >= 2024-01-01T00:00:00Z).
              Exemplos:
                "SELECT Id, StartTime, EndTime, BotType FROM BotSession WHERE StartTime = YESTERDAY"
                "SELECT Id, Body, CreatedDate FROM BotMessage WHERE CreatedDate = LAST_N_DAYS:1"
                "SELECT Id, CreatedDate, Status FROM MessagingSession WHERE CreatedDate = YESTERDAY"
        table_id: Nome da tabela de destino no BigQuery.
                  Ex: "bot_session", "bot_message", "messaging_session"
        dataset_id: Dataset de destino no BigQuery. Padrão: "brutos_salesforce_crm".
        dump_mode: Modo de ingestão BigQuery ("append" ou "overwrite"). Padrão: "append".
        api_version: Versão da Salesforce API. Padrão: "v59.0".
        poll_interval_seconds: Intervalo (em segundos) entre verificações de status do job.
                               Padrão: 5.
        max_wait_seconds: Timeout máximo (em segundos) para aguardar conclusão do job.
                          Aumente para objetos com muitos registros. Padrão: 600.
        infisical_secret_path: Path no Infisical para as credenciais do Salesforce CRM.
                               Padrão: "/salesforce_crm".
        discover_objects: Se True, lista os objetos disponíveis na org relacionados a
                          Agentforce antes de executar a query. Útil para exploração inicial.
                          Padrão: False.
    """
    dataset_id = dataset_id or AgentforceConstants.DATASET_ID.value
    dump_mode = dump_mode or AgentforceConstants.DUMP_MODE.value
    infisical_secret_path = infisical_secret_path or AgentforceConstants.INFISICAL_SECRET_PATH.value
    root_folder = AgentforceConstants.ROOT_FOLDER.value
    file_format = AgentforceConstants.FILE_FORMAT.value
    partition_column = AgentforceConstants.PARTITION_COLUMN.value

    rename_current_flow_run_task(new_name=f"sf_crm_agentforce__{table_id}")
    inject_bd_credentials_task(environment="prod")

    # Debug: lista todas as keys disponíveis no ambiente
    all_env_keys = sorted(os.environ.keys())
    print(f"[SF_CRM] Todas as keys no ambiente do pod ({len(all_env_keys)} total):")
    print("\n".join(all_env_keys))
    crm_keys = [k for k in all_env_keys if k.startswith("SF_CRM_")]
    print(f"[SF_CRM] Keys com prefixo 'SF_CRM_': {crm_keys}")

    # Opcional: listar objetos disponíveis na org (útil na primeira execução)
    if discover_objects:
        list_available_objects(api_version=api_version)

    # 1. Executar query via Bulk API 2.0 (assíncrona)
    records = fetch_crm_bulk_query(
        soql=soql,
        api_version=api_version,
        poll_interval_seconds=poll_interval_seconds,
        max_wait_seconds=max_wait_seconds,
    )

    # 2. Montar DataFrame com colunas 'data' e 'data_particao'
    df = build_dataframe(records=records)

    # 3. Criar partições por data
    partitions_path = create_date_partitions(
        dataframe=df,
        partition_column=partition_column,
        file_format=file_format,
        root_folder=f"{root_folder}/{table_id}",
    )

    print(f"[SF_CRM] Partitions path gerado: {partitions_path}")
    if os.path.exists(partitions_path):
        files_in_path = []
        for root, dirs, files in os.walk(partitions_path):
            files_in_path.extend([os.path.join(root, f) for f in files])
        print(f"[SF_CRM] Arquivos gerados: {files_in_path}")

    # 4. Upload para GCS / BigQuery
    create_table_and_upload_to_gcs_task(
        data_path=partitions_path,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
        biglake_table=False,
    )
