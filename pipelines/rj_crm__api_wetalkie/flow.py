
from pathlib import Path

from prefect import flow
from prefect.logging import get_logger

from pipelines.rj_crm__api_wetalkie.constants import WetalkieConstants
from pipelines.rj_crm__api_wetalkie.helpers.tasks import (
    access_api,
    create_date_partitions,
)
from pipelines.rj_crm__api_wetalkie.tasks import (
    criar_dataframe_de_lista,
    get_attendances,
    processar_json_e_transcrever_audios,
)
from utils.tasks import publish_table_in_bq, upload_files_to_gcs
from utils.types import Cloud, Table
from utils.utils import capture_options

logger = get_logger("toolkit")


@flow(log_prints=True, flow_run_name="{table_id}_{dataset_id}")
async def rj_crm__api_wetalkie(
    *,
    cloud: Cloud,
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

    # Injetar credenciais do BD

    api = access_api(
        infisical_secret_path,
        "wetalkie_url",
        "wetalkie_user",
        "wetalkie_pass",
        login_route=WetalkieConstants.API_LOGIN_ROUTE.value,
    )

    raw_attendances = get_attendances(api)

    # Check if there's data to process - return early if empty
    if raw_attendances.empty:
        logger.info(
            "No attendances found from API. Flow completed successfully with no data to process."
        )
        return

    # Processar JSON e transcrever áudios
    processed_data = processar_json_e_transcrever_audios(dados_entrada=raw_attendances)

    df = criar_dataframe_de_lista(processed_data)

    # Criar partições por data
    partitions_path = create_date_partitions(
        dataframe=df,
        partition_column=partition_column,
        file_format=file_format,
        root_folder=root_folder,
    )

    # Upload para GCS e BigQuery
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
