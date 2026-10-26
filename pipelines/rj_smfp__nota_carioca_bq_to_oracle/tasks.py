"""Tasks da carga BigQuery → GCS → SQL*Loader → Oracle."""

import time
from datetime import datetime
from typing import cast
from uuid import UUID

from prefect import task
from prefect.artifacts import create_progress_artifact, update_progress_artifact
from prefect.cache_policies import NO_CACHE
from prefect.logging import get_logger
from prefect.runtime import flow_run

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.constants import DBT_POLL_SECONDS
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers import (
    bigquery,
    dbt,
    inmemory,
    oracle,
    oracle_ddl,
    oracle_read,
    slots,
    slots_oracle,
    sqlldr,
    sqlldr_progress,
)
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.bigquery import TableSchema
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.columns import LoadPlan, build_load_plan
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.notify import current as current_notifier
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.structure import plan_structure, slot_indexes
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.structure_ddl import describe_partitioning
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.structure_models import StructurePlan
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.types import Dbt, Load, Source
from utils.runs import PrefectRuns, start_deployment_run

logger = get_logger("toolkit")


@task(cache_policy=NO_CACHE)
def run_dbt_task(options: Dbt) -> datetime:
    """Run the dbt deployment as a subflow and wait for success.

    Return when it ended (UTC).
    """
    parameters = dbt.resolve_dbt_parameters(options.parameters)
    run = start_deployment_run(deployment=options.deployment, parameters=parameters)
    link = f" ({run.url})" if run.url else ""
    logger.info(f"dbt iniciado: run {run.name} [{run.id}] do deployment {options.deployment}{link}")
    waiter = dbt.RunWaiter(
        runs=PrefectRuns(),
        timeout_seconds=options.timeout_minutes * 60,
        poll_seconds=DBT_POLL_SECONDS,
        report=logger.info,
    )
    finished_at = waiter.wait(run.id)
    logger.info(f"dbt concluído às {finished_at:%Y-%m-%d %H:%M:%S} UTC")
    return finished_at


@task
def list_tables_task(source: Source) -> list[str]:
    """Return the tables that were asked for or, with none, all of the dataset."""
    return source.table_ids or bigquery.list_tables(project=source.project, dataset_id=source.dataset_id)


@task(cache_policy=NO_CACHE)
def export_snapshot_task(
    source: Source, load: Load, table_ids: list[str], dbt_finished_at: datetime | None
) -> dict[str, bigquery.TableSnapshot]:
    """Export every table as one BigQuery snapshot, under a prefix of this flow run."""
    prefix = f"{source.dataset_id}/{flow_run.get_id()}"
    request = bigquery.SnapshotRequest(
        project=source.project,
        dataset_id=source.dataset_id,
        table_ids=table_ids,
        bucket=source.bucket,
        prefix=prefix,
        quiet_seconds=load.bigquery_quiet_minutes * 60,
        dbt_finished_at=dbt_finished_at,
    )
    snapshot = bigquery.export_consistent_snapshot(request, report=logger.info)
    for table in snapshot.values():
        total = sqlldr_progress.format_size(sum(exported.size for exported in table.files))
        rows = sqlldr_progress.format_count(table.schema.num_rows)
        logger.info(
            f"{table.table_id}: foto do BigQuery de {table.last_modified:%Y-%m-%d %H:%M:%S} UTC, "
            + f"{rows} linhas, {len(table.files)} arquivos ({total})"
        )
    return snapshot


@task(cache_policy=NO_CACHE)
def plan_load_task(load: Load, table_id: str, table_schema: TableSchema) -> LoadPlan:
    """Build the load plan from the original Oracle table and the BigQuery schema."""
    config = oracle.read_oracle_config()
    template = oracle_read.fetch_template_columns(
        config=config,
        template_schema=oracle.validate_identifier(load.template_schema or config.schema),
        table=oracle.validate_identifier(table_id),
    )
    plan = build_load_plan(table_schema.fields, template, load.excluded_columns, load.raw_text_encoding)
    if plan.excluded:
        logger.info(
            f"{table_id}: colunas da original que não são criadas na BQLOAD_ (ROWID ou excluídas): {plan.excluded}"
        )
    if plan.ignored:
        logger.info(f"{table_id}: colunas do BigQuery que não existem na original e são ignoradas: {plan.ignored}")
    return plan


@task(cache_policy=NO_CACHE)
def plan_structure_task(load: Load, table_id: str, plan: LoadPlan) -> StructurePlan:
    """Read the tablespace, partitions and indexes of the original.

    Then plan those of the target.
    """
    config = oracle.read_oracle_config()
    template = oracle_read.fetch_template_layout(
        config=config,
        template_schema=oracle.validate_identifier(load.template_schema or config.schema),
        table=oracle.validate_identifier(table_id),
    )
    structure = plan_structure(template, [column.name for column in plan.columns], oracle.TABLE_PREFIX)
    indexes = ", ".join(index.name for index in structure.layout.indexes) or "nenhum"
    logger.info(
        f"{table_id}: tablespace {structure.layout.tablespace or '(padrão do schema)'}, "
        + f"{describe_partitioning(structure.layout.partitioning)}, {structure.layout.inmemory or 'NO INMEMORY'}; "
        + f"índices criados após a carga: {indexes}"
    )
    if structure.skipped_indexes:
        logger.info(
            f"{table_id}: índices da original não replicados (internos de view materializada): "
            + f"{list(structure.skipped_indexes)}"
        )
    return structure


@task(cache_policy=NO_CACHE)
def resolve_slots_task(table_id: str) -> slots.SlotPlan:
    """Find the physical table in use and the one that receives the load.

    Fix interrupted swaps.
    """
    base = oracle.oracle_table_name(table_id)
    plan, realigned = slots_oracle.resolve_slots(config=oracle.read_oracle_config(), base=base)
    if realigned:
        logger.info(f"{base}: troca anterior incompleta; sinônimos de {realigned} voltaram para {plan.active}")
    logger.info(f"{base}: em uso {plan.active or '(nenhuma; primeira carga em A/B)'}; a carga vai para {plan.inactive}")
    return plan


@task(cache_policy=NO_CACHE)
def ensure_oracle_table_task(
    source: Source, table_id: str, table: str, plan: LoadPlan, structure: StructurePlan
) -> str:
    """Cria ou confere a tabela física que recebe a carga e retorna o nome dela."""
    definition = oracle_ddl.TableDefinition(
        columns=plan.columns, layout=structure.layout, source=f"{source.project}.{source.dataset_id}.{table_id}"
    )
    logger.info(oracle_ddl.ensure_table(config=oracle.read_oracle_config(), table=table, definition=definition))
    return table


@task(cache_policy=NO_CACHE)
def drop_oracle_indexes_task(table: str) -> str:
    """Drop the indexes of the target before the parallel load, and return its name."""
    dropped = oracle_ddl.drop_managed_indexes(config=oracle.read_oracle_config(), table=table)
    logger.info(
        f"{table}: índices apagados para a carga: {dropped}" if dropped else f"{table}: sem índices para apagar"
    )
    return table


@task(cache_policy=NO_CACHE)
def truncate_oracle_table_task(table: str) -> str:
    """Esvazia a tabela de destino e retorna o nome dela."""
    oracle_ddl.truncate_table(config=oracle.read_oracle_config(), table=table)
    return table


@task(cache_policy=NO_CACHE)
def load_into_oracle_task(
    table: str, plan: LoadPlan, files: list[bigquery.ExportedFile], source: Source, load: Load
) -> int:
    """Load the files with SQL*Loader direct path and report the progress.

    Return the rows that were loaded.
    """
    sessions = load.sessions
    job = sqlldr.LoadJob(table=table, fields=plan.fields, bucket=source.bucket, files=files, sessions=sessions)
    artifact_id = cast("UUID", create_progress_artifact(progress=0.0, description=f"Carga de {table}"))
    total_size = sqlldr_progress.format_size(sum(exported.size for exported in files))
    logger.info(
        f"Carga de {table} iniciada: {len(files)} arquivos ({total_size}) em {min(sessions, len(files))} sessão(ões)"
    )

    notifier = current_notifier()

    def report(snapshot: sqlldr_progress.ProgressSnapshot) -> None:
        logger.info(sqlldr_progress.format_progress(table, snapshot))
        update_progress_artifact(artifact_id=artifact_id, progress=sqlldr_progress.progress_percent(snapshot))
        if notifier is not None:
            notifier.loading(snapshot)

    result = sqlldr.load_from_gcs(
        config=oracle.read_oracle_config(),
        job=job,
        on_progress=report,
        progress_interval_seconds=load.progress_interval_seconds,
    )
    update_progress_artifact(artifact_id=artifact_id, progress=100.0)
    sessions_text = ", ".join(
        f"sessão {item.index}: {item.files} arquivos, {sqlldr_progress.format_count(item.rows)} linhas"
        for item in result.sessions
    )
    logger.info(
        f"Carga de {table} concluída: {sqlldr_progress.format_count(result.rows)} linhas em "
        + f"{sqlldr_progress.format_duration(result.elapsed_seconds)} ({sessions_text})"
    )
    return result.rows


@task(cache_policy=NO_CACHE)
def validate_row_count_task(table: str, table_schema: TableSchema, loaded_rows: int, load: Load) -> int:
    """Check that BigQuery, SQL*Loader and Oracle have the same number of rows.

    `COUNT(*)` in Oracle runs with the hint `PARALLEL(parallel_degree)`. Raise
    `ValueError` if the counts differ.
    """
    expected = table_schema.num_rows
    oracle_rows = oracle_ddl.count_rows(
        config=oracle.read_oracle_config(), table=table, parallel_degree=load.index_parallel_degree
    )
    counts = ", ".join(
        f"{label}={sqlldr_progress.format_count(value)}"
        for label, value in (("BigQuery", expected), ("SQL*Loader", loaded_rows), ("Oracle", oracle_rows))
    )
    if not expected == loaded_rows == oracle_rows:
        raise ValueError(f"{table}: contagens divergentes: {counts}")
    logger.info(f"{table}: contagens conferidas: {counts}")
    return oracle_rows


@task(cache_policy=NO_CACHE)
def grant_access_task(table: str) -> str:
    """Concede aos consumidores o acesso à tabela física e retorna o nome dela."""
    slots_oracle.grant_access(config=oracle.read_oracle_config(), table=table)
    grantees = ", ".join(f"{grantee} ({privileges})" for grantee, privileges in slots.CONSUMER_GRANTS)
    logger.info(f"{table}: acesso concedido a {grantees}")
    return table


@task(cache_policy=NO_CACHE)
def delete_gcs_files_task(source: Source, files: list[bigquery.ExportedFile]) -> None:
    """Delete the exported files from GCS.

    The flow submits this task in the background, after the validation of the count, and
    waits for its result before it ends: a failure of the deletion still fails the flow,
    and it does not delay the creation of the indexes.
    """
    names = [exported.name for exported in files]
    bigquery.delete_blobs(bucket=source.bucket, blob_names=names)
    logger.info(f"Removidos {len(files)} arquivos temporários de gs://{source.bucket}")


@task(cache_policy=NO_CACHE)
def create_oracle_indexes_task(table: str, structure: StructurePlan, slot: str, load: Load) -> str:
    """Create the indexes of the physical table, one at a time, and return its name.

    They get its suffix.
    """
    config = oracle.read_oracle_config()
    parallel_degree = load.index_parallel_degree
    indexes = slot_indexes(structure.layout.indexes, slot)
    if not indexes:
        logger.info(f"{table}: a original não tem índices para replicar")
        return table
    artifact_id = cast("UUID", create_progress_artifact(progress=0.0, description=f"Índices de {table}"))
    notifier = current_notifier()
    if notifier is not None:
        notifier.indexes(0, len(indexes))
    started = time.monotonic()
    for position, index in enumerate(indexes, start=1):
        logger.info(
            f"{table}: criando índice {position}/{len(indexes)} {index.name} {index.description} "
            + f"com PARALLEL {parallel_degree}"
        )
        index_started = time.monotonic()
        oracle_ddl.create_index(config=config, table=table, index=index, parallel_degree=parallel_degree)
        update_progress_artifact(artifact_id=artifact_id, progress=100.0 * position / len(indexes))
        if notifier is not None:
            notifier.indexes(position, len(indexes))
        took = sqlldr_progress.format_duration(time.monotonic() - index_started)
        logger.info(f"{table}: índice {index.name} criado em {took}")
    elapsed = sqlldr_progress.format_duration(time.monotonic() - started)
    logger.info(f"{table}: {len(indexes)} índice(s) criado(s) em {elapsed}")
    return table


@task(cache_policy=NO_CACHE)
def gather_oracle_stats_task(table: str, load: Load) -> str:
    """Gather the statistics of the physical table, and return its name."""
    started = time.monotonic()
    oracle_ddl.gather_table_stats(
        config=oracle.read_oracle_config(), table=table, parallel_degree=load.index_parallel_degree
    )
    logger.info(f"{table}: estatísticas coletadas em {sqlldr_progress.format_duration(time.monotonic() - started)}")
    return table


@task(cache_policy=NO_CACHE)
def record_load_task(
    plan: slots.SlotPlan, table: str, source: Source, table_id: str, snapshot_modified: datetime
) -> slots.SlotPlan:
    """Write the loaded snapshot in the table comment, and return the swap plan."""
    comment = slots_oracle.record_load(
        config=oracle.read_oracle_config(),
        table=table,
        source=f"{source.project}.{source.dataset_id}.{table_id}",
        snapshot_modified=snapshot_modified,
    )
    logger.info(f"{table}: carga concluída e registrada ({comment})")
    return plan


@task(cache_policy=NO_CACHE)
def start_inmemory_population_task(table: str, structure: StructurePlan, load: Load) -> None:
    """Ask Oracle to populate the physical table in the In-Memory.

    It runs only if the table uses In-Memory and the wait is on.
    """
    if structure.layout.inmemory is None or load.inmemory_wait_minutes <= 0:
        return
    errors = inmemory.start_population(
        config=oracle.read_oracle_config(),
        table=table,
        partitioned=structure.layout.partitioning is not None,
    )
    logger.info(
        f"{table}: população do In-Memory iniciada" if not errors else f"{table}: população não iniciada: {errors}"
    )


@task(cache_policy=NO_CACHE)
def wait_inmemory_task(plans: list[slots.SlotPlan], load: Load) -> list[slots.SlotPlan]:
    """Wait for the loaded tables to reach the In-Memory before the swap.

    Return the plans.
    """
    wait_minutes = load.inmemory_wait_minutes

    if wait_minutes <= 0:
        logger.info("Espera pelo In-Memory desligada (inmemory_wait_minutes=0); a troca segue direto.")
        return plans
    started = time.monotonic()
    complete = inmemory.wait_for_population(
        config=oracle.read_oracle_config(),
        tables=[plan.inactive for plan in plans],
        timeout_seconds=wait_minutes * 60,
        report=logger.info,
    )
    if complete:
        logger.info(
            f"Tabelas carregadas inteiras no In-Memory em {sqlldr_progress.format_duration(time.monotonic() - started)}"
        )
    return plans


@task(cache_policy=NO_CACHE)
def swap_synonyms_task(plans: list[slots.SlotPlan]) -> None:
    """Point the synonyms of all tables to the ones just loaded, one table at a time."""
    config = oracle.read_oracle_config()
    for plan in plans:
        actions = slots_oracle.swap_synonyms(config=config, plan=plan)
        logger.info(f"{plan.base}: troca feita: {'; '.join(actions)}")
        kept = f"{plan.active} fica com a carga anterior" if plan.active else "não havia tabela A/B anterior"
        logger.info(f"{plan.base}: em uso agora {plan.inactive}; {kept}")


@task(cache_policy=NO_CACHE)
def refresh_synonyms_task(table_ids: list[str]) -> None:
    """Point the synonyms of all the owners again to the tables in use, with no load."""
    config = oracle.read_oracle_config()
    for table_id in table_ids:
        base = oracle.oracle_table_name(table_id)
        actions = slots_oracle.refresh_synonyms(config=config, base=base)
        logger.info(f"{base}: sinônimos reapontados para a tabela em uso, sem carga: {'; '.join(actions)}")
