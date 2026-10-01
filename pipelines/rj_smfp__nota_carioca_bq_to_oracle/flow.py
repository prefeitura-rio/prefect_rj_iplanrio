"""Flow for rj_smfp__nota_carioca_bq_to_oracle."""

from prefect import flow

from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.tasks import (
    create_oracle_indexes_task,
    delete_gcs_files_task,
    drop_oracle_indexes_task,
    ensure_oracle_table_task,
    export_snapshot_task,
    gather_oracle_stats_task,
    grant_access_task,
    list_tables_task,
    load_into_oracle_task,
    plan_load_task,
    plan_structure_task,
    record_load_task,
    resolve_slots_task,
    start_inmemory_population_task,
    swap_synonyms_task,
    truncate_oracle_table_task,
    validate_row_count_task,
    wait_inmemory_task,
)


@flow(log_prints=True)
def rj_smfp__nota_carioca_bq_to_oracle(  # noqa: PLR0913
    project: str = "rj-iplanrio-dia",
    dataset_id: str = "nota_carioca_staging",
    table_ids: list[str] | None = None,
    gcs_bucket: str = "rj-iplanrio-dia-bq-to-oracle",
    infisical_secret_path: str = "/db-oracle-nota-fiscal",
    sqlldr_sessions: int = 2,
    template_schema: str | None = None,
    excluded_template_columns: list[str] | None = None,
    progress_interval_seconds: int = 30,
    index_parallel_degree: int = 4,
    bigquery_quiet_minutes: int = 5,
    inmemory_wait_minutes: int = 30,
) -> None:
    rename_current_flow_run_task(new_name=dataset_id)
    inject_bd_credentials_task(environment="prod")
    tables = list_tables_task(project=project, dataset_id=dataset_id, table_ids=table_ids)
    snapshot = export_snapshot_task(
        project=project,
        dataset_id=dataset_id,
        table_ids=tables,
        bucket=gcs_bucket,
        quiet_minutes=bigquery_quiet_minutes,
    )

    loaded = []
    for table_id in tables:
        exported = snapshot[table_id]
        plan = plan_load_task(
            infisical_secret_path=infisical_secret_path,
            template_schema=template_schema,
            excluded_template_columns=excluded_template_columns,
            table_id=table_id,
            table_schema=exported.schema,
        )
        structure = plan_structure_task(
            infisical_secret_path=infisical_secret_path, template_schema=template_schema, table_id=table_id, plan=plan
        )
        slots = resolve_slots_task(infisical_secret_path=infisical_secret_path, table_id=table_id)
        oracle_table = ensure_oracle_table_task(
            infisical_secret_path=infisical_secret_path,
            project=project,
            dataset_id=dataset_id,
            table_id=table_id,
            table=slots.inactive,
            plan=plan,
            structure=structure,
        )
        unindexed_table = drop_oracle_indexes_task(infisical_secret_path=infisical_secret_path, table=oracle_table)
        empty_table = truncate_oracle_table_task(infisical_secret_path=infisical_secret_path, table=unindexed_table)
        loaded_rows = load_into_oracle_task(
            infisical_secret_path=infisical_secret_path,
            table=empty_table,
            plan=plan,
            bucket=gcs_bucket,
            files=exported.files,
            sessions=sqlldr_sessions,
            progress_interval_seconds=progress_interval_seconds,
        )
        validated_rows = validate_row_count_task(
            infisical_secret_path=infisical_secret_path,
            table=empty_table,
            table_schema=exported.schema,
            loaded_rows=loaded_rows,
        )
        delete_gcs_files_task(project=project, bucket=gcs_bucket, files=exported.files, wait_for=[validated_rows])
        indexed_table = create_oracle_indexes_task(
            infisical_secret_path=infisical_secret_path,
            table=empty_table,
            structure=structure,
            slot=slots.slot,
            parallel_degree=index_parallel_degree,
            wait_for=[validated_rows],
        )
        analyzed_table = gather_oracle_stats_task(
            infisical_secret_path=infisical_secret_path, table=indexed_table, parallel_degree=index_parallel_degree
        )
        granted_table = grant_access_task(infisical_secret_path=infisical_secret_path, table=analyzed_table)
        ready = record_load_task(
            infisical_secret_path=infisical_secret_path,
            plan=slots,
            table=granted_table,
            source=f"{project}.{dataset_id}.{table_id}",
            snapshot_modified=exported.last_modified,
        )
        start_inmemory_population_task(
            infisical_secret_path=infisical_secret_path,
            table=granted_table,
            structure=structure,
            wait_minutes=inmemory_wait_minutes,
        )
        loaded.append(ready)

    populated = wait_inmemory_task(
        infisical_secret_path=infisical_secret_path, plans=loaded, wait_minutes=inmemory_wait_minutes
    )
    swap_synonyms_task(infisical_secret_path=infisical_secret_path, plans=populated)
