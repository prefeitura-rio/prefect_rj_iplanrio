# -*- coding: utf-8 -*-
"""Fluxo responsável por preparar imagens e QRCodes do SISBICHO."""

from iplanrio.pipelines_utils.bd import create_table_and_upload_to_gcs_task
from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.logging import log
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect import flow

from pipelines.rj_iplanrio__sisbicho_images.constants import SisbichoImagesConstants
from pipelines.rj_iplanrio__sisbicho_images.tasks import (
    build_output_dataframe_task,
    extract_qrcode_payload_task,
    fetch_sisbicho_media_task,
    upload_pet_images_task,
)
from pipelines.rj_smas__api_datametrica_agendamentos.utils.tasks import create_date_partitions


@flow(log_prints=True)
def rj_iplanrio__sisbicho_images(
    source_dataset_id: str | None = None,
    source_table_id: str | None = None,
    dataset_id: str | None = None,
    table_id: str | None = None,
    dump_mode: str | None = None,
    materialize_after_dump: bool | None = None,
    storage_bucket: str | None = None,
    storage_prefix: str | None = None,
    billing_project_id: str | None = None,
    credential_bucket: str | None = None,
    limit: int | None = None,
):
    """Extrai o payload do QRCode e publica as fotos dos pets do SISBICHO."""

    constants = SisbichoImagesConstants

    dataset_id = dataset_id or constants.TARGET_DATASET.value
    table_id = table_id or constants.TARGET_TABLE.value
    dump_mode = dump_mode or constants.DUMP_MODE.value
    storage_bucket = storage_bucket or constants.IMAGE_BUCKET.value
    storage_prefix = storage_prefix or constants.IMAGE_PREFIX.value
    billing_project_id = billing_project_id or constants.BILLING_PROJECT.value
    credential_bucket = credential_bucket or constants.CREDENTIAL_BUCKET.value
    source_dataset_id = source_dataset_id or constants.SOURCE_DATASET.value
    source_table_id = source_table_id or constants.SOURCE_TABLE.value
    materialize_after_dump = (
        materialize_after_dump if materialize_after_dump is not None else constants.MATERIALIZE_AFTER_DUMP.value
    )

    rename_flow_run = rename_current_flow_run_task(new_name=f"{table_id}_{dataset_id}")
    credentials = inject_bd_credentials_task(environment="prod", wait_for=[rename_flow_run])

    dataframe = fetch_sisbicho_media_task(
        billing_project_id=billing_project_id,
        credential_bucket=credential_bucket,
        dataset_id=source_dataset_id,
        table_id=source_table_id,
        limit=limit,
        wait_for=[credentials],
    )

    if dataframe.empty:
        log("Nenhum registro com QRCode ou foto encontrado. Fluxo finalizado sem alterações.")
        return []

    dataframe = extract_qrcode_payload_task(dataframe)
    dataframe = upload_pet_images_task(
        dataframe=dataframe,
        storage_bucket=storage_bucket,
        storage_prefix=storage_prefix,
        billing_project_id=billing_project_id,
    )

    output_dataframe = build_output_dataframe_task(dataframe)

    if output_dataframe.empty:
        log("Após processamento não há dados para gravar. Fluxo encerrado.")
        return []

    partitions_path = create_date_partitions(
        dataframe=output_dataframe,
        partition_column=constants.PARTITION_COLUMN.value,
        file_format=constants.FILE_FORMAT.value,
        root_folder=constants.ROOT_FOLDER.value,
    )

    create_table_and_upload_to_gcs_task(
        data_path=partitions_path,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
        biglake_table=True,
    )

    if materialize_after_dump:
        log("Nenhuma materialização configurada para este fluxo. Ignorando flag materialize_after_dump.")

    log("Fluxo concluído com sucesso.")
    return []
