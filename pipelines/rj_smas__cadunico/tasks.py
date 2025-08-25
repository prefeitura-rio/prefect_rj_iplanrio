# -*- coding: utf-8 -*-
from typing import List

from prefect import task

from pipelines.rj_smas__cadunico.utils_dump import (
    get_existing_partitions,
    get_files_to_ingest,
    ingest_files,
    need_to_ingest,
)


@task
def get_existing_partitions_task(
    prefix: str, bucket_name: str, dataset_id: str, table_id: str
) -> List[str]:
    return get_existing_partitions(
        prefix=prefix, bucket_name=bucket_name, dataset_id=dataset_id, table_id=table_id
    )


@task()
def get_files_to_ingest_task(
    prefix: str, partitions: List[str], bucket_name: str
) -> List[str]:
    return get_files_to_ingest(
        prefix=prefix, partitions=partitions, bucket_name=bucket_name
    )


@task
def need_to_ingest_task(files_to_ingest: list) -> bool:
    return need_to_ingest(files_to_ingest=files_to_ingest)


@task
def ingest_files_task(
    files_to_ingest: List[str],
    bucket_name: str,
    dataset_id: str,
    table_id: str,
    max_concurrent: int = 3,
) -> None:
    return ingest_files(
        files_to_ingest=files_to_ingest,
        bucket_name=bucket_name,
        dataset_id=dataset_id,
        table_id=table_id,
        max_concurrent=max_concurrent,
    )
