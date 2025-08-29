# -*- coding: utf-8 -*-
# ruff: noqa
from typing import List, Optional

from iplanrio.pipelines_utils.constants import NOT_SET
from iplanrio.pipelines_utils.env import (
    get_database_username_and_password_from_secret_env,
)
from prefect import task

from pipelines.rj_segovi__dump_db_1746.utils_break_query import (
    format_partitioned_query,
)
from pipelines.rj_segovi__dump_db_1746.utils_database import (
    parse_comma_separated_string_to_list,
)
from pipelines.rj_segovi__dump_db_1746.utils_dump import (
    dump_upload_batch,
)


@task
def get_database_username_and_password_from_secret_task(infisical_secret_path: str):
    return get_database_username_and_password_from_secret_env(secret_path=infisical_secret_path)


@task
def parse_comma_separated_string_to_list_task(text: Optional[str]) -> List[str]:
    return parse_comma_separated_string_to_list(text)


@task
def dump_upload_batch_task(
    database_type: str,
    hostname: str,
    port: int,
    user: str,
    password: str,
    database: str,
    queries: List[dict],
    batch_size: int,
    dataset_id: str,
    table_id: str,
    dump_mode: str,
    charset: str = NOT_SET,
    partition_columns: List[str] = [],
    batch_data_type: str = "csv",
    biglake_table: bool = True,
    log_number_of_batches: int = 100,
    retry_dump_upload_attempts: int = 2,
    max_concurrency: int = 1,
    only_staging_dataset: bool = False,
):
    dump_upload_batch(
        database_type=database_type,
        hostname=hostname,
        port=port,
        user=user,
        password=password,
        database=database,
        queries=queries,
        batch_size=batch_size,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
        charset=charset,
        partition_columns=partition_columns,
        batch_data_type=batch_data_type,
        biglake_table=biglake_table,
        log_number_of_batches=log_number_of_batches,
        retry_dump_upload_attempts=retry_dump_upload_attempts,
        max_concurrency=max_concurrency,
        only_staging_dataset=only_staging_dataset,
    )


@task
def format_partitioned_query_task(
    query: str,
    dataset_id: str,
    table_id: str,
    database_type: str,
    partition_columns: Optional[List[str]] = None,
    lower_bound_date: Optional[str] = None,
    date_format: Optional[str] = None,
    break_query_start: Optional[str] = None,
    break_query_end: Optional[str] = None,
    break_query_frequency: Optional[str] = None,
    wait: Optional[str] = None,
):
    return format_partitioned_query(
        query=query,
        dataset_id=dataset_id,
        table_id=table_id,
        database_type=database_type,
        partition_columns=partition_columns,
        lower_bound_date=lower_bound_date,
        date_format=date_format,
        break_query_start=break_query_start,
        break_query_end=break_query_end,
        break_query_frequency=break_query_frequency,
        wait=wait,
    )
