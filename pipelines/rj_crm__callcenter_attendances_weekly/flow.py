
from pathlib import Path

from prefect import flow

from pipelines.rj_crm__callcenter_attendances_weekly.constants import (
    CallCenterAttendancesConstants,
)
from pipelines.rj_crm__callcenter_attendances_weekly.tasks import (
    access_api,
    calculate_date_range,
    create_date_partitions,
    criar_dataframe_de_lista,
    filter_new_attendances,
    get_existing_attendance_keys,
    get_weekly_attendances,
    processar_json_e_transcrever_audios,
)
from utils.tasks import publish_table_in_bq, upload_files_to_gcs
from utils.types import Cloud, Table
from utils.utils import capture_options


@flow(log_prints=True, flow_run_name="{table_id}_{dataset_id}_weekly")
async def rj_crm__callcenter_attendances_weekly(
    *,
    cloud: Cloud,
    # Parâmetros opcionais para override manual na UI
    dataset_id: str | None = None,
    table_id: str | None = None,
    dump_mode: str | None = None,
    materialize_after_dump: bool | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    transcribe_audio: bool = True,
    infisical_secret_path: str = "/wetalkie",
    date_interval: int = 7,
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

    dataset_id = dataset_id or CallCenterAttendancesConstants.DATASET_ID.value
    table_id = table_id or CallCenterAttendancesConstants.TABLE_ID.value
    dump_mode = dump_mode or CallCenterAttendancesConstants.DUMP_MODE.value
    materialize_after_dump = (
        materialize_after_dump
        if materialize_after_dump is not None
        else CallCenterAttendancesConstants.MATERIALIZE_AFTER_DUMP.value
    )

    partition_column = CallCenterAttendancesConstants.PARTITION_COLUMN.value
    file_format = CallCenterAttendancesConstants.FILE_FORMAT.value
    root_folder = CallCenterAttendancesConstants.ROOT_FOLDER.value
    biglake_table = CallCenterAttendancesConstants.BIGLAKE_TABLE.value
    billing_project_id = CallCenterAttendancesConstants.BILLING_PROJECT_ID.value
    date_interval = date_interval or CallCenterAttendancesConstants.DATE_INTERVAL.value



    date_range = calculate_date_range(
        start_date=start_date, end_date=end_date, interval=date_interval
    )

    api = access_api(
        infisical_secret_path,
        "wetalkie_url",
        "wetalkie_user",
        "wetalkie_pass",
        login_route=CallCenterAttendancesConstants.API_LOGIN_ROUTE.value,
    )

    raw_attendances = get_weekly_attendances(
        api=api, start_date=date_range["start_date"], end_date=date_range["end_date"]
    )

    if raw_attendances.empty:
        print(
            f"No attendances found from API for period {date_range['start_date']} to {date_range['end_date']}. Flow completed successfully with no data to process."
        )
        return
    existing_keys = get_existing_attendance_keys(
        dataset_id=dataset_id,
        table_id=table_id,
        start_date=date_range["start_date"],
        end_date=date_range["end_date"],
        billing_project_id=billing_project_id,
    )

    filtered_attendances = filter_new_attendances(
        raw_attendances=raw_attendances,
        existing_keys=existing_keys,
    )

    if filtered_attendances.empty:
        print(
            f"No new attendances to process for period {date_range['start_date']} to {date_range['end_date']}. All data already exists. Flow completed successfully."
        )
        return

    if transcribe_audio:
        processed_data = processar_json_e_transcrever_audios(
            dados_entrada=filtered_attendances
        )
    else:
        # processed_data = filtered_attendances.to_dict("records")
        processed_data = filtered_attendances.copy()

    df = criar_dataframe_de_lista(processed_data)

    print(
        f"Processed {len(df)} new attendances for period {date_range['start_date']} to {date_range['end_date']}"
    )

    partitions_path = create_date_partitions(
        dataframe=df,
        partition_column=partition_column,
        file_format=file_format,
        root_folder=root_folder,
    )
    print("Force deploy")
    table = Table(dataset_id=dataset_id, table_id=table_id)
    await upload_files_to_gcs(
        Path(partitions_path),
        table,
        bucket=cloud["bucket"],
        dump_mode=dump_mode,
        source_format="csv",
        options=capture_options(dump_mode),
    )
    await publish_table_in_bq(table, cloud)
    print(
        f"Weekly attendances pipeline completed successfully for {date_range['start_date']} to {date_range['end_date']}"
    )
