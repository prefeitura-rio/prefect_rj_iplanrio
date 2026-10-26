"""Flow for rj_smfp__nota_carioca_bq_to_oracle."""

import time
from typing import Literal, cast

from prefect import flow
from prefect.exceptions import TerminationSignal
from prefect.runtime import flow_run
from prefect.settings import PREFECT_API_URL, PREFECT_UI_URL

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.cleanup import raise_failures
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.dbt import should_run_dbt
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.discord_format import flow_run_url
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.notify import LoadNotifier, NotifierConfig
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.stages import Step, TableStep
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
    refresh_synonyms_task,
    resolve_slots_task,
    run_dbt_task,
    start_inmemory_population_task,
    swap_synonyms_task,
    truncate_oracle_table_task,
    validate_row_count_task,
    wait_inmemory_task,
)
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.types import Dbt, Load, Source

DEFAULT_SOURCE = Source()
DEFAULT_LOAD = Load()
DEFAULT_DBT = Dbt()


@flow(log_prints=True, flow_run_name="{source.dataset_id}")
def rj_smfp__nota_carioca_bq_to_oracle(
    *,
    source: Source = DEFAULT_SOURCE,
    load: Load = DEFAULT_LOAD,
    dbt: Dbt = DEFAULT_DBT,
    mode: Literal["full", "synonyms_only"] = "full",
    discord_notifications: bool = True,
) -> None:
    """Load the tables of a BigQuery dataset into Oracle, with slots A and B.

    It swaps synonyms at the end.

    The flow exports every table as one consistent snapshot to GCS, loads each one into
    the idle slot with SQL*Loader, checks the row count against BigQuery, builds the
    indexes of the original table, and swaps the synonyms of all the tables at the end.
    With `dbt.enabled`, a dbt run goes first and the export waits for it. In
    `synonyms_only` mode there is no load: the synonyms go back to the tables in use. A
    Discord message follows the run, unless `discord_notifications` is off.

    The files of a table go only after its validation, which raises if the count
    differs, and the deletion runs next to the indexes. The flow waits for it at the
    end, so a failure still fails the flow. The indexes wait for the validation with
    `wait_for`, because they use no value of it.
    """
    dbt_enabled = should_run_dbt(dbt.enabled, mode)
    notifier = LoadNotifier.create(
        NotifierConfig(
            dataset_id=source.dataset_id,
            url=flow_run_url(
                cast("str | None", PREFECT_UI_URL.value()),
                cast("str | None", PREFECT_API_URL.value()),
                str(flow_run.get_id()),
            ),
            run_name=source.dataset_id,
            table_ids=tuple(source.table_ids or ()),
            sessions=load.sessions,
            run_dbt=dbt_enabled,
            mode=mode,
            enabled=discord_notifications,
            cancellations=(TerminationSignal,),
        )
    )
    with notifier.guard():
        notifier.begin()
        dbt_finished_at = None
        if dbt_enabled:
            dbt_started = time.monotonic()
            dbt_finished_at = run_dbt_task(options=dbt)
            notifier.dbt_finished(time.monotonic() - dbt_started)
        tables = list_tables_task(source=source)
        notifier.set_tables(dict.fromkeys(tables, 1))
        if mode == "synonyms_only":
            refresh_synonyms_task(table_ids=tables)
            return
        notifier.enter(Step.EXPORT)
        snapshot = export_snapshot_task(source=source, load=load, table_ids=tables, dbt_finished_at=dbt_finished_at)
        notifier.exported(
            {name: sum(file.size for file in snapshot[name].files) for name in tables},
            {name: snapshot[name].schema.num_rows for name in tables},
        )
        notifier.enter(Step.TABLES)

        loaded, cleanups = [], []
        for table_id in tables:
            exported = snapshot[table_id]
            notifier.table_step(table_id, TableStep.PREPARATION)
            plan = plan_load_task(load=load, table_id=table_id, table_schema=exported.schema)
            structure = plan_structure_task(load=load, table_id=table_id, plan=plan)
            slots = resolve_slots_task(table_id=table_id)
            notifier.table_step(table_id, TableStep.PREPARATION, slot=slots.slot)
            notifier.set_physical_table(table_id, slots.inactive)
            oracle_table = ensure_oracle_table_task(
                source=source, table_id=table_id, table=slots.inactive, plan=plan, structure=structure
            )
            empty_table = truncate_oracle_table_task(table=drop_oracle_indexes_task(table=oracle_table))
            notifier.table_step(table_id, TableStep.LOAD)
            loaded_rows = load_into_oracle_task(
                table=empty_table, plan=plan, files=exported.files, source=source, load=load
            )
            notifier.table_loaded(table_id, loaded_rows)
            notifier.table_step(table_id, TableStep.VALIDATION)
            validated_rows = validate_row_count_task(
                table=empty_table, table_schema=exported.schema, loaded_rows=loaded_rows, load=load
            )
            notifier.table_validated(table_id, validated_rows)
            cleanups.append(delete_gcs_files_task.submit(source=source, files=exported.files))
            notifier.table_step(table_id, TableStep.INDEXES)
            indexed_table = create_oracle_indexes_task(
                table=empty_table, structure=structure, slot=slots.slot, load=load, wait_for=[validated_rows]
            )
            notifier.table_step(table_id, TableStep.STATS)
            analyzed_table = gather_oracle_stats_task(table=indexed_table, load=load)
            notifier.table_step(table_id, TableStep.ACCESS)
            granted_table = grant_access_task(table=analyzed_table)
            loaded.append(
                record_load_task(
                    plan=slots,
                    table=granted_table,
                    source=source,
                    table_id=table_id,
                    snapshot_modified=exported.last_modified,
                )
            )
            notifier.table_step(table_id, TableStep.INMEMORY)
            start_inmemory_population_task(table=granted_table, structure=structure, load=load)
            notifier.table_done(table_id, validated_rows)

        notifier.enter(Step.INMEMORY_WAIT)
        populated = wait_inmemory_task(plans=loaded, load=load)
        notifier.enter(Step.SWAP)
        swap_synonyms_task(plans=populated)
        raise_failures(cleanups)
