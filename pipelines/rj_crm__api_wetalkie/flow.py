# -*- coding: utf-8 -*-
"""
Flow migrado do Prefect 1.4 para 3.0 - CRM API Wetalkie

⚠️ ATENÇÃO: Algumas funções da biblioteca prefeitura_rio NÃO têm equivalente no iplanrio:
- handler_initialize_sentry: SEM EQUIVALENTE
- LocalDaskExecutor: Removido no Prefect 3.0
- KubernetesRun: Removido no Prefect 3.0 (configurado no YAML)
- GCS storage: Removido no Prefect 3.0 (configurado no YAML)
- Parameter: Substituído por parâmetros de função
- case (conditional execution): Substituído por if/else padrão
"""

from iplanrio.pipelines_utils.bd import create_table_and_upload_to_gcs_task
from iplanrio.pipelines_utils.dbt import execute_dbt_task
from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect import flow

from pipelines.rj_crm__api_wetalkie.constants import WetalkieConstants
from pipelines.rj_crm__api_wetalkie.tasks import (
    criar_dataframe_de_lista,
    get_attendances,
    processar_json_e_transcrever_audios,
)
from pipelines.rj_crm__api_wetalkie.utils.tasks import (
    access_api,
    create_date_partitions,
    skip_flow_if_empty,
)


@flow(log_prints=True)
def rj_crm__api_wetalkie(
    # Parâmetros opcionais para override manual na UI
    dataset_id: str | None = None,
    table_id: str | None = None,
    dump_mode: str | None = None,
    materialize_after_dump: bool | None = None,
    infisical_secret_path: str = "/wetalkie",
):
    """
    Flow para extrair dados de atendimentos da API Wetalkie e carregar no BigQuery.

    Este flow coleta os atendimentos (attendances) da API Wetalkie, processa áudios
    encontrados nas mensagens transcrevendo-os, e carrega os dados no BigQuery.

    Args:
        dataset_id: ID do dataset no BigQuery (default: brutos_wetalkie)
        table_id: ID da tabela no BigQuery (default: fluxos_ura)
        dump_mode: Modo de dump (default: append)
        materialize_after_dump: Se deve materializar após dump (default: True)
        infisical_secret_path: Caminho dos secrets no Infisical (default: /wetalkie)
    """

    # Usar valores dos constants como padrão para parâmetros
    dataset_id = dataset_id or WetalkieConstants.DATASET_ID.value
    table_id = table_id or WetalkieConstants.TABLE_ID.value
    dump_mode = dump_mode or WetalkieConstants.DUMP_MODE.value
    materialize_after_dump = (
        materialize_after_dump
        if materialize_after_dump is not None
        else WetalkieConstants.MATERIALIZE_AFTER_DUMP.value
    )

    partition_column = WetalkieConstants.PARTITION_COLUMN.value
    file_format = WetalkieConstants.FILE_FORMAT.value
    root_folder = WetalkieConstants.ROOT_FOLDER.value
    biglake_table = WetalkieConstants.BIGLAKE_TABLE.value

    # Renomear flow run para melhor identificação
    rename_flow_run = rename_current_flow_run_task(new_name=f"{table_id}_{dataset_id}")

    # Injetar credenciais do BD
    crd = inject_bd_credentials_task(environment="prod")  # noqa

    api = access_api(
        infisical_secret_path,
        "wetalkie_url",
        "wetalkie_user",
        "wetalkie_pass",
        login_route=WetalkieConstants.API_LOGIN_ROUTE.value,
    )

    raw_attendances = get_attendances(api)

    # Verificar se há dados para processar
    validated_attendances = skip_flow_if_empty(
        data=raw_attendances,
        message="No attendances found from API. Skipping flow execution.",
    )

    # Processar JSON e transcrever áudios
    processed_data = processar_json_e_transcrever_audios(
        dados_entrada=validated_attendances
    )

    # Converter lista processada para DataFrame
    df = criar_dataframe_de_lista(processed_data)

    # Criar partições por data
    partitions_path = create_date_partitions(
        dataframe=df,
        partition_column=partition_column,
        file_format=file_format,
        root_folder=root_folder,
    )

    # Upload para GCS e BigQuery
    create_table_and_upload_to_gcs_task(
        data_path=partitions_path,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
        biglake_table=biglake_table,
    )

    # Materializar com DBT se necessário
    if materialize_after_dump:
        dbt_select = WetalkieConstants.DBT_SELECT.value
        execute_dbt_task(select=dbt_select, target="prod")
