# -*- coding: utf-8 -*-

from iplanrio.pipelines_utils.bd import create_table_and_upload_to_gcs_task
from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect import flow

from pipelines.rj_smas__callcenter_attendances_weekly.constants import CallCenterAttendancesConstants
from pipelines.rj_smas__callcenter_attendances_weekly.tasks import (
    calculate_date_range,
    criar_dataframe_de_lista,
    get_weekly_attendances,
)
from pipelines.rj_smas__callcenter_attendances_weekly.utils.tasks import (
    access_api,
    create_date_partitions,
)
from pipelines.rj_crm__api_wetalkie.tasks import processar_json_e_transcrever_audios


@flow(log_prints=True)
def rj_smas__callcenter_attendances_weekly(
    # Parâmetros opcionais para override manual na UI
    dataset_id: str | None = None,
    table_id: str | None = None,
    dump_mode: str | None = None,
    materialize_after_dump: bool | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    infisical_secret_path: str = "/wetalkie",
):
    """
    Flow para extrair dados de atendimentos da API Wetalkie em janelas semanais e carregar no BigQuery.

    Este flow coleta os atendimentos (attendances) da API Wetalkie para um período específico,
    processa áudios encontrados nas mensagens transcrevendo-os, e carrega os dados no BigQuery.

    Args:
        dataset_id: ID do dataset no BigQuery (default from constants)
        table_id: ID da tabela no BigQuery (default from constants)
        dump_mode: Modo de dump (default from constants)
        materialize_after_dump: Se deve materializar após dump (default from constants)
        start_date: Data de início no formato YYYY-MM-DD (None = calcular automaticamente)
        end_date: Data de fim no formato YYYY-MM-DD (None = calcular automaticamente)
        infisical_secret_path: Caminho dos secrets no Infisical (default: /wetalkie)
    """

    # Usar valores dos constants como padrão para parâmetros
    dataset_id = dataset_id or CallCenterAttendancesConstants.DATASET_ID.value
    table_id = table_id or CallCenterAttendancesConstants.TABLE_ID.value
    dump_mode = dump_mode or CallCenterAttendancesConstants.DUMP_MODE.value
    materialize_after_dump = (
        materialize_after_dump if materialize_after_dump is not None
        else CallCenterAttendancesConstants.MATERIALIZE_AFTER_DUMP.value
    )

    partition_column = CallCenterAttendancesConstants.PARTITION_COLUMN.value
    file_format = CallCenterAttendancesConstants.FILE_FORMAT.value
    root_folder = CallCenterAttendancesConstants.ROOT_FOLDER.value
    biglake_table = CallCenterAttendancesConstants.BIGLAKE_TABLE.value

    # Renomear flow run para melhor identificação
    rename_flow_run = rename_current_flow_run_task(new_name=f"{table_id}_{dataset_id}_weekly")

    # Injetar credenciais do BD
    crd = inject_bd_credentials_task(environment="prod")  # noqa

    # Calcular período de datas (7 dias anteriores se não fornecido)
    date_range = calculate_date_range(start_date=start_date, end_date=end_date)

    # Acessar API
    api = access_api(
        infisical_secret_path,
        "wetalkie_url",
        "wetalkie_user",
        "wetalkie_pass",
        login_route=CallCenterAttendancesConstants.API_LOGIN_ROUTE.value,
    )

    # Buscar atendimentos para o período especificado
    raw_attendances = get_weekly_attendances(
        api=api,
        start_date=date_range["start_date"],
        end_date=date_range["end_date"]
    )

    # Check if there's data to process - return early if empty
    if raw_attendances.empty:
        print(f"No attendances found from API for period {date_range['start_date']} to {date_range['end_date']}. Flow completed successfully with no data to process.")
        return

    # Processar JSON e transcrever áudios (mesmo processamento da pipeline original)
    processed_data = processar_json_e_transcrever_audios(dados_entrada=raw_attendances)

    df = criar_dataframe_de_lista(processed_data)

    print(f"Processed {len(df)} attendances for period {date_range['start_date']} to {date_range['end_date']}")

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

    print(f"Weekly attendances pipeline completed successfully for {date_range['start_date']} to {date_range['end_date']}")

    # if materialize_after_dump:
    #    dbt_select = CallCenterAttendancesConstants.DBT_SELECT.value
    #    execute_dbt_task(select=dbt_select, target="prod")