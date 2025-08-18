# -*- coding: utf-8 -*-
"""
Flow migrado do Prefect 1.4 para 3.0 - SMAS API Datametrica Agendamentos

⚠️ ATENÇÃO: Algumas funções da biblioteca prefeitura_rio NÃO têm equivalente no iplanrio:
- handler_initialize_sentry: SEM EQUIVALENTE
- task_run_dbt_model_task: SEM EQUIVALENTE (precisa ser implementado se necessário)
- LocalDaskExecutor: Removido no Prefect 3.0
- KubernetesRun: Removido no Prefect 3.0 (configurado no YAML)
- GCS storage: Removido no Prefect 3.0 (configurado no YAML)
- Parameter: Substituído por parâmetros de função
- case (conditional execution): Substituído por if/else padrão
"""

from iplanrio.pipelines_utils.bd import create_table_and_upload_to_gcs_task
from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from pipelines.rj_smas__api_datametrica_agendamentos.constants import (
    DatametricaConstants,
)
from pipelines.rj_smas__api_datametrica_agendamentos.tasks import (
    calculate_target_date,
    convert_agendamentos_to_dataframe,
    fetch_agendamentos_from_api,
    get_datametrica_credentials,
    transform_agendamentos_data,
)
from pipelines.rj_smas__api_datametrica_agendamentos.utils.tasks import (
    create_date_partitions,
)
from prefect import flow


@flow(log_prints=True)
def rj_smas__api_datametrica_agendamentos(
    # Parâmetros opcionais para override manual na UI
    dataset_id: str | None = None,
    table_id: str | None = None,
    dump_mode: str | None = None,
    materialize_after_dump: bool | None = None,
    date: str | None = None,
    infisical_secret_path: str | None = "/api-datametrica",
):
    """
    Flow para extrair agendamentos da API Datametrica e carregar no BigQuery.

    Regra de negócio para datas:
    - Dias normais: busca dados para 2 dias à frente
    - Quinta e sexta-feira: busca dados para 4 dias à frente (cobrindo fim de semana)

    Args:
        dataset_id: ID do dataset no BigQuery (default: brutos_data_metrica)
        table_id: ID da tabela no BigQuery (default: agendamentos_cadunico)
        dump_mode: Modo de dump (default: append)
        materialize_after_dump: Se deve materializar após dump (default: True)
        date: Data para buscar agendamentos no formato YYYY-MM-DD (default: None = usa regra de negócio)
        infisical_secret_path: Caminho dos secrets no Infisical (default: None)
    """

    # Usar valores dos constants como padrão
    dataset_id = dataset_id or DatametricaConstants.DATASET_ID.value
    table_id = table_id or DatametricaConstants.TABLE_ID.value
    dump_mode = dump_mode or DatametricaConstants.DUMP_MODE.value
    materialize_after_dump = (
        materialize_after_dump
        if materialize_after_dump is not None
        else DatametricaConstants.MATERIALIZE_AFTER_DUMP.value
    )

    partition_column = DatametricaConstants.PARTITION_COLUMN.value
    file_format = DatametricaConstants.FILE_FORMAT.value
    root_folder = DatametricaConstants.ROOT_FOLDER.value
    biglake_table = DatametricaConstants.BIGLAKE_TABLE.value

    # Renomear flow run para melhor identificação
    rename_flow_run = rename_current_flow_run_task(new_name=f"{table_id}_{dataset_id}")

    # Injetar credenciais do BD
    crd = inject_bd_credentials_task(environment="prod")  # noqa

    # Obter credenciais da API Datametrica
    credentials = get_datametrica_credentials(
        infisical_secret_path=infisical_secret_path
    )

    # Calcular data target baseada na regra de negócio (a menos que date seja fornecido explicitamente)
    target_date = date if date is not None else calculate_target_date()

    # Buscar dados da API
    raw_data = fetch_agendamentos_from_api(credentials=credentials, date=target_date)

    # Transformar os dados
    processed_data = transform_agendamentos_data(raw_data)

    # Converter para DataFrame
    df = convert_agendamentos_to_dataframe(processed_data)

    print(df.columns)
    print(df)
    # Criar partições por data
    partitions_path = create_date_partitions(
        dataframe=df,
        partition_column=partition_column,
        file_format=file_format,
        root_folder=root_folder,
    )

    # Upload para GCS e BigQuery
    # create_table_and_upload_to_gcs_task(
    #    data_path=partitions_path,
    #    dataset_id=dataset_id,
    #    table_id=table_id,
    #    dump_mode=dump_mode,
    #    biglake_table=biglake_table,
    # )

    # Executar DBT se materialize_after_dump for True
    # ⚠️ NOTA: task_run_dbt_model_task não tem equivalente em iplanrio
    # Implementar se necessário ou executar DBT separadamente

    if materialize_after_dump:
        print(
            f"⚠️ DBT materialization requested but not implemented - run manually for dataset_id={dataset_id}, table_id={table_id}"
        )
        # TODO: Implementar task_run_dbt_model_task equivalente se necessário
        # run_dbt = task_run_dbt_model_task(
        #     dataset_id=dataset_id,
        #     table_id=table_id,
        # )
