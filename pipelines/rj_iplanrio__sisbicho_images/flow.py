"""Fluxo responsável por preparar imagens e QRCodes do SISBICHO."""

from pathlib import Path

from prefect import flow
from prefect.logging import get_logger

from pipelines.rj_iplanrio__sisbicho_images.constants import SisbichoImagesConstants
from pipelines.rj_iplanrio__sisbicho_images.helpers.tasks import create_date_partitions
from pipelines.rj_iplanrio__sisbicho_images.tasks import (
    fetch_sisbicho_media_task,
    process_single_batch,
)
from utils.tasks import publish_table_in_bq, upload_files_to_gcs
from utils.types import Cloud, Table
from utils.utils import capture_options

logger = get_logger("toolkit")


@flow(log_prints=True, flow_run_name="{table_id}_{dataset_id}")
async def rj_iplanrio__sisbicho_images(
    *,
    cloud: Cloud,
    source_dataset_id: str | None = None,
    source_table_id: str | None = None,
    dataset_id: str | None = None,
    table_id: str | None = None,
    dump_mode: str | None = None,
    materialize_after_dump: bool | None = None,
    storage_bucket: str | None = None,
    storage_prefix: str | None = None,
    billing_project_id: str | None = None,
    storage_project_id: str | None = None,
    credential_bucket: str | None = None,
    batch_size: int = 1000,
    max_records: int | None = None,
):
    constants = SisbichoImagesConstants

    logger.info("Fluxo rj_iplanrio__sisbicho_images – versão OCT-20")

    dataset_id = dataset_id or constants.TARGET_DATASET.value
    table_id = table_id or constants.TARGET_TABLE.value
    dump_mode = dump_mode or constants.DUMP_MODE.value
    storage_bucket = storage_bucket or constants.IMAGE_BUCKET.value
    storage_prefix = storage_prefix or constants.IMAGE_PREFIX.value
    billing_project_id = billing_project_id or constants.BILLING_PROJECT.value
    storage_project_id = storage_project_id or constants.STORAGE_PROJECT.value
    credential_bucket = credential_bucket or constants.CREDENTIAL_BUCKET.value
    source_dataset_id = source_dataset_id or constants.SOURCE_DATASET.value
    source_table_id = source_table_id or constants.SOURCE_TABLE.value
    materialize_after_dump = (
        materialize_after_dump
        if materialize_after_dump is not None
        else constants.MATERIALIZE_AFTER_DUMP.value
    )

    target_dataset_for_queries = (
        dataset_id if dataset_id.endswith("_staging") else f"{dataset_id}_staging"
    )
    dataset_id_for_upload = (
        dataset_id[:-8] if dataset_id.endswith("_staging") else dataset_id
    )


    client, source_table, target_table, identifier_field, total_count = (
        fetch_sisbicho_media_task(
            billing_project_id=billing_project_id,
            credential_bucket=credential_bucket,
            source_dataset_id=source_dataset_id,
            source_table_id=source_table_id,
            target_dataset_id=dataset_id,
            target_table_id=table_id,
            batch_size=batch_size,
            max_records=max_records,
        )
    )

    if total_count == 0:
        logger.info(
            "Nenhum registro com QRCode ou foto encontrado. Fluxo finalizado sem alterações."
        )
        return []

    logger.info(f"Processando {total_count} registros em lotes de {batch_size}")
    total_processed = 0

    for offset in range(0, total_count, batch_size):
        logger.info(
            f"Processando lote {offset // batch_size + 1} de {(total_count + batch_size - 1) // batch_size}"
        )

        batch_output = process_single_batch(
            client=client,
            source_table=source_table,
            target_table=target_table,
            identifier_field=identifier_field,
            offset=offset,
            batch_size=batch_size,
            storage_bucket=storage_bucket,
            storage_prefix=storage_prefix,
            billing_project_id=billing_project_id,
            storage_project_id=storage_project_id,
        )

        if not batch_output.empty:
            partitions_path = create_date_partitions(
                dataframe=batch_output,
                partition_column=constants.PARTITION_COLUMN.value,
                file_format=constants.FILE_FORMAT.value,
                root_folder=constants.ROOT_FOLDER.value,
                append_mode=True,
            )
            try:
                table = Table(dataset_id=dataset_id_for_upload, table_id=table_id)
                await upload_files_to_gcs(
                    Path(partitions_path),
                    table,
                    bucket=cloud["bucket"],
                    dump_mode=dump_mode,
                    source_format=constants.FILE_FORMAT.value,
                    options=capture_options(dump_mode),
                )
                await publish_table_in_bq(table, cloud)
            except Exception as exc:
                error_msg = str(exc).lower()
                # Detecta tabela vazia que causa erro no basedosdados
                if (
                    "cannot query hive partitioned data" in error_msg
                    and "without any associated files" in error_msg
                ):
                    logger.info(
                        "[RECOVERY] Erro de tabela vazia detectado em create_table. Deletando tabela..."
                    )
                    try:
                        client.delete_table(target_table, not_found_ok=True)
                        logger.info(
                            f"[RECOVERY] Tabela {target_table} deletada. Tentando criar novamente..."
                        )
                        table = Table(dataset_id=dataset_id_for_upload, table_id=table_id)
                        await upload_files_to_gcs(
                            Path(partitions_path),
                            table,
                            bucket=cloud["bucket"],
                            dump_mode=dump_mode,
                            source_format=constants.FILE_FORMAT.value,
                            options=capture_options(dump_mode),
                        )
                        await publish_table_in_bq(table, cloud)
                        logger.info("[RECOVERY] Tabela criada com sucesso após recovery.")
                    except Exception as retry_exc:
                        logger.info(f"[ERRO] Falha no retry após deletar tabela: {retry_exc}")
                        raise
                else:
                    raise

            total_processed += len(batch_output)

            logger.info(
                f"Lote gravado no BigQuery. Total acumulado: {total_processed} registros."
            )

    if total_processed == 0:
        logger.info("Após processamento não há dados para gravar. Fluxo encerrado.")
        return []

    logger.info(f"Processamento concluído. Total de registros: {total_processed}")

    if materialize_after_dump:
        logger.info(
            "Nenhuma materialização configurada para este fluxo. Ignorando flag materialize_after_dump."
        )

    logger.info("Fluxo concluído com sucesso!!")
    return
