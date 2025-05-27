# -*- coding: utf-8 -*-
from prefect import flow, task

from iplanrio.pipelines_utils.logging import log
from iplanrio.pipelines_utils.env import (
    get_database_username_and_password_from_secret_env,
)
from pipelines.rj_smfp.dump_db_ergon.schedules import ergon_daily_schedule
from iplanrio.pipelines_utils.constants import NOT_SET


@task
def get_database_username_and_password_from_secret_tastk(infisical_secret_path: str):
    return get_database_username_and_password_from_secret_env(
        infisical_secret_path=infisical_secret_path
    )


@flow(log_prints=True)
def dump_db_ergon(
    db_database: str = None,
    db_host: str = None,
    db_port: str = None,
    db_type: str = None,
    db_charset: str = NOT_SET,
    query: str = None,
    dataset_id: str = None,
    table_id: str = None,
    partition_date_format: str = None,
    partition_columns: str = None,
    infisical_secret_path: str = None,
    lower_bound_date: str = None,
    break_query_frequency: str = None,
    break_query_start: str = None,
    break_query_end: str = None,
    retry_dump_upload_attempts: str = 1,
    batch_size: str = 50000,
    batch_data_type: str = "csv",
    biglake_table: str = True,
    log_number_of_batches: int = 100,
):
    secrets = get_database_username_and_password_from_secret_tastk(
        infisical_secret_path=infisical_secret_path
    )
    # log all parameters
    log(
        {
            "db_database": db_database,
            "db_host": db_host,
            "db_port": db_port,
            "db_type": db_type,
            "db_charset": db_charset,
            "query": query,
            "dataset_id": dataset_id,
            "table_id": table_id,
            "partition_date_format": partition_date_format,
            "partition_columns": partition_columns,
            "infisical_secret_path": infisical_secret_path,
            "lower_bound_date": lower_bound_date,
            "break_query_frequency": break_query_frequency,
            "break_query_start": break_query_start,
            "break_query_end": break_query_end,
            "retry_dump_upload_attempts": retry_dump_upload_attempts,
            "batch_size": batch_size,
            "batch_data_type": batch_data_type,
            "biglake_table": biglake_table,
            "log_number_of_batches": log_number_of_batches,
            "db_username": secrets["DB_USERNAME"],
        }
    )


if __name__ == "__main__":
    dump_db_ergon.serve(name="dump_db_ergon", schedule=ergon_daily_schedule)
