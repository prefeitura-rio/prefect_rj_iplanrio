# -*- coding: utf-8 -*-
import asyncio
import json
from typing import List

from iplanrio.pipelines_utils.gcs import (
    list_blobs_with_prefix,
    parse_blobs_to_partition_list,
)
from iplanrio.pipelines_utils.logging import log
from prefect import task
from prefect.utilities.asyncutils import run_sync_in_worker_thread

from pipelines.rj_smas__cadunico.utils import (
    ingest_file_sync,
    parse_partition_from_filename,
)


@task
def get_existing_partitions(prefix: str, bucket_name: str, dataset_id: str, table_id: str) -> List[str]:
    """
    Lista as partições já processadas na área de staging.

    Args:
        prefix (str): Prefixo do caminho para listar partições.
        bucket_name (str): Nome do bucket GCS.

    Returns:
        List[str]: Lista de partições no formato `YYYY-MM-DD`.
    """
    # List blobs in staging area

    log(f"Listing blobs in staging area with prefix {bucket_name}/{prefix}")
    log(f"https://console.cloud.google.com/storage/browser/{bucket_name}/staging/{dataset_id}/{table_id}")

    staging_blobs = list_blobs_with_prefix(bucket_name=bucket_name, prefix=prefix)
    log(f"Found {len(staging_blobs)} blobs in staging area")

    # Extract partition information from blobs
    staging_partitions = parse_blobs_to_partition_list(staging_blobs)
    staging_partitions = list(set(staging_partitions))
    log(f"Staging partitions {len(staging_partitions)}: {staging_partitions}")
    return staging_partitions


@task()
def get_files_to_ingest(prefix: str, partitions: List[str], bucket_name: str) -> List[str]:
    """
    Identifica arquivos ZIP novos na área raw que ainda não foram processados.

    Args:
        prefix (str): Prefixo do caminho na área raw.
        partitions (List[str]): Lista de partições já processadas.
        bucket_name (str): Nome do bucket GCS.

    Returns:
        List[str]: Lista de nomes de blobs para ingerir.
    """
    # List blobs in raw area
    raw_blobs = list_blobs_with_prefix(bucket_name=bucket_name, prefix=prefix)
    log(f"https://console.cloud.google.com/storage/browser/{bucket_name}/{prefix}")
    log(f"Found {len(raw_blobs)} blobs in raw area")

    # Filter ZIP files
    raw_blobs = [blob for blob in raw_blobs if blob.name and blob.name.lower().endswith(".zip")]
    log(f"ZIP blobs {len(raw_blobs)}")

    # Extract partition information from blobs
    raw_partitions = []
    raw_partitions_blobs = []
    for blob in raw_blobs:
        if not blob.name:
            continue
        try:
            raw_partitions.append(parse_partition_from_filename(blob.name))
            raw_partitions_blobs.append(blob)
        except Exception as e:
            log(f"Failed to parse partition from blob {blob.name}: {e}", "warning")
    log(f"Raw partitions: {raw_partitions}")

    # Filter files that are not in the staging area
    files_to_ingest = []
    partitions_to_ingest = []
    log_to_injest = {}
    for blob, partition in zip(raw_partitions_blobs, raw_partitions, strict=False):
        if partition not in partitions:
            files_to_ingest.append(blob.name)
            partitions_to_ingest.append(partition)
            log_to_injest[partition] = blob.name

    log_to_injest_str = json.dumps(log_to_injest, indent=2, ensure_ascii=False)
    log(f"Found {len(files_to_ingest)} files to ingest\n{log_to_injest_str}")
    return files_to_ingest


@task
def need_to_ingest(files_to_ingest: list) -> bool:
    """
    Verifica se existem arquivos para ingerir.
    """
    variable = files_to_ingest != []
    log(f"Need to ingest: {variable}")
    return variable


@task
def ingest_files(
    files_to_ingest: List[str],
    bucket_name: str,
    dataset_id: str,
    table_id: str,
    max_concurrent: int = 3,
) -> None:
    """
    Processa múltiplos arquivos ZIP de forma assíncrona com controle de concorrência.

    Args:
        files_to_ingest (List[str]): Lista de nomes de blobs para ingerir.
        bucket_name (str): Nome do bucket GCS.
        dataset_id (str): ID do dataset de destino.
        table_id (str): ID da tabela de destino.
        max_concurrent (int): Número máximo de downloads/processamentos simultâneos.
    """
    if not files_to_ingest:
        log("No files to ingest")
        return

    async def _run_async():
        log(f"Starting async ingestion of {len(files_to_ingest)} files with max {max_concurrent} concurrent tasks")

        semaphore = asyncio.Semaphore(max_concurrent)

        async def _run_with_semaphore(blob_name):
            async with semaphore:
                return await run_sync_in_worker_thread(
                    ingest_file_sync,
                    blob_name,
                    bucket_name,
                    dataset_id,
                    table_id,
                )

        tasks = []
        for blob_name in files_to_ingest:
            task = _run_with_semaphore(blob_name)
            tasks.append(task)

        await asyncio.gather(*tasks)
        log(f"Completed ingestion of {len(files_to_ingest)} files")

    asyncio.run(_run_async())
