"""
Upload eai messages history to BQ..
"""

from pathlib import Path
from typing import Optional

from prefect import flow
from toolkit.dbt import DbtRun

from pipelines.rj_iplanrio__eai_history.tasks import (
    fetch_history_data,
    get_last_update,
)
from utils.tasks import publish_table_in_bq, run_dbt, upload_files_to_gcs
from utils.types import Cloud, Table
from utils.utils import capture_options

DBT_REPOSITORY = "https://github.com/prefeitura-rio/queries-rj-iplanrio"


@flow(log_prints=True, flow_run_name="{environment}")
async def rj_iplanrio__eai_history(
    *,
    cloud: Cloud,  # noqa
    last_update: Optional[str] = None,
    session_timeout_seconds: Optional[int] = 3600,
    use_whatsapp_format: bool = False,
    dataset_id: str = "brutos_eai_logs",
    table_id: str = "history",
    max_user_save_limit: int = 100,
    environment: str = "staging",
    dbt_select: str = "raw_eai_logs_history",
    sql_limit: Optional[int] = None,
):
    last_update_task = get_last_update(
        dataset_id=dataset_id,
        table_id=table_id,
        last_update=last_update,
        environment=environment,
    )

    data_path = fetch_history_data(
        last_update=last_update_task,
        session_timeout_seconds=session_timeout_seconds,
        use_whatsapp_format=use_whatsapp_format,
        max_user_save_limit=max_user_save_limit,
        environment=environment,
        sql_limit=sql_limit,
    )

    if data_path:
        table = Table(dataset_id=dataset_id, table_id=table_id)
        await upload_files_to_gcs(
            Path(data_path),
            table,
            bucket=cloud["bucket"],
            dump_mode="append",
            source_format="csv",
            options=capture_options("append"),
        )
        await publish_table_in_bq(table, cloud)
        await run_dbt(DbtRun(repository=DBT_REPOSITORY, command="run", target="prod", select=dbt_select))


if __name__ == "__main__":
    rj_iplanrio__eai_history()
