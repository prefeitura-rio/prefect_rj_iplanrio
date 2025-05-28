# -*- coding: utf-8 -*-
from typing import Optional
from iplanrio.pipelines_utils.constants import NOT_SET
from iplanrio.pipelines_utils.env import (
    get_database_username_and_password_from_secret_env,
)
from iplanrio.pipelines_utils.logging import log
from prefect import flow, task

from pipelines.rj_smfp.dump_db_ergon.schedules import ergon_daily_schedules


@task
def get_database_username_and_password_from_secret_tastk(infisical_secret_path: str):
    return get_database_username_and_password_from_secret_env(
        infisical_secret_path=infisical_secret_path
    )


@flow(log_prints=True)
def dump_db_ergon(
    db_database: str,
    db_host: str,
    db_port: str,
    db_type: str,
    execute_query: str,
    dataset_id: str,
    table_id: str,
    infisical_secret_path: str,
    partition_date_format: Optional[str] = None,
    partition_columns: Optional[str] = None,
    lower_bound_date: Optional[str] = None,
    break_query_frequency: Optional[str] = None,
    break_query_start: Optional[str] = None,
    break_query_end: Optional[str] = None,
    db_charset: str = NOT_SET,
    retry_dump_upload_attempts: int = 1,
    batch_size: int = 50000,
    batch_data_type: str = "csv",
    biglake_table: bool = True,
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
            "execute_query": execute_query,
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
    dump_db_ergon.serve(
        name="dump_db_ergon",
        schedules=ergon_daily_schedules,
    )


# dump_db_ergon.deploy(
#     name="dump_db_ergon",
#     schedules=ergon_daily_schedules,
#     image="d116626/prefect-flows:0.1.4",
#     work_pool_name="default",
#     work_queue_name="default",
#     job_variables={
#         "command": "uv run prefect flow-run execute",
#     },
# )
