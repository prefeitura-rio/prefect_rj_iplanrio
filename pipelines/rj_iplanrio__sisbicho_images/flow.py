# -*- coding: utf-8 -*-
"""Fluxo responsável por preparar imagens e QRCodes do SISBICHO."""

from iplanrio.pipelines_utils.bd import create_table_and_upload_to_gcs_task
from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.logging import log
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect import flow

from pipelines.rj_iplanrio__sisbicho_images.constants import SisbichoImagesConstants
from pipelines.rj_iplanrio__sisbicho_images.tasks import (
    fetch_sisbicho_media_task,
    process_single_batch,
)
from pipelines.rj_iplanrio__sisbicho_images.utils.tasks import create_date_partitions


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
    storage_project_id: str | None = None,
    credential_bucket: str | None = None,
    batch_size: int = 1000,
    max_records: int | None = None,
):
    constants = SisbichoImagesConstants

    log("Fluxo rj_iplanrio__sisbicho_images – versão OCT-20")

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

    rename_flow_run = rename_current_flow_run_task(
        new_name=f"{table_id}_{target_dataset_for_queries}"
    )
    credentials = inject_bd_credentials_task(
        environment="prod", wait_for=[rename_flow_run]
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
            wait_for=[credentials],
        )
    )

    if total_count == 0:
        log(
            "Nenhum registro com QRCode ou foto encontrado. Fluxo finalizado sem alterações."
        )
        return []

    log(f"Processando {total_count} registros em lotes de {batch_size}")
    total_processed = 0

    for offset in range(0, total_count, batch_size):
        log(
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
                create_table_and_upload_to_gcs_task(
                    data_path=partitions_path,
                    dataset_id=dataset_id_for_upload,
                    table_id=table_id,
                    dump_mode=dump_mode,
                    source_format=constants.FILE_FORMAT.value,
                    biglake_table=False,
                )
            except Exception as exc:
                error_msg = str(exc).lower()
                # Detecta tabela vazia que causa erro no basedosdados
                if (
                    "cannot query hive partitioned data" in error_msg
                    and "without any associated files" in error_msg
                ):
                    log(
                        "[RECOVERY] Erro de tabela vazia detectado em create_table. Deletando tabela..."
                    )
                    try:
                        client.delete_table(target_table, not_found_ok=True)
                        log(
                            f"[RECOVERY] Tabela {target_table} deletada. Tentando criar novamente..."
                        )
                        create_table_and_upload_to_gcs_task(
                            data_path=partitions_path,
                            dataset_id=dataset_id_for_upload,
                            table_id=table_id,
                            dump_mode=dump_mode,
                            source_format=constants.FILE_FORMAT.value,
                            biglake_table=False,
                        )
                        log("[RECOVERY] Tabela criada com sucesso após recovery.")
                    except Exception as retry_exc:
                        log(f"[ERRO] Falha no retry após deletar tabela: {retry_exc}")
                        raise
                else:
                    raise

            total_processed += len(batch_output)

            log(
                f"Lote gravado no BigQuery. Total acumulado: {total_processed} registros."
            )

    if total_processed == 0:
        log("Após processamento não há dados para gravar. Fluxo encerrado.")
        return []

    log(f"Processamento concluído. Total de registros: {total_processed}")

    if materialize_after_dump:
        log(
            "Nenhuma materialização configurada para este fluxo. Ignorando flag materialize_after_dump."
        )

    log("Fluxo concluído com sucesso!!")
    return
