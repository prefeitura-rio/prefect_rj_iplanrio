"""Flow for rj_smfp__nota_carioca_oracle_to_bq.

Dois modos no mesmo deployment. O pai (``table_id`` nulo) tira a foto, planeja, confere a memória e, com
``parallel_tables``, lança um filho por tabela, cada um no seu pod, e só publica depois que todos validaram.
O filho (``table_id`` preenchido) extrai, carrega e valida uma tabela e nunca toca nas finais. Com
``parallel_tables=False`` o pai faz tudo em sequência no próprio pod.

Os padrões de memória (``workers=2``, ``worker_memory_mb=640``, ``pod_memory_mb=1792``) cabem no REQUEST de 2 GiB por
pod do template de job do K3s aplicado: passar do request deixa o scheduler superalocar o nó, que ficou NotReady.
A publicação é tudo ou nada: todas as tabelas são conferidas antes do primeiro copy e, se um copy falhar, as já
publicadas voltam ao estado anterior por time travel.
"""

from prefect import flow
from prefect.runtime import flow_run

from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.constants import DEFAULT_TABLES
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.tasks import (
    check_memory_budget_task,
    cleanup_task,
    drop_leftover_chunk_tasks_task,
    ensure_exclusive_task,
    extract_table_task,
    launch_children_task,
    load_table_task,
    plan_table_task,
    publish_tables_task,
    stamp_validated_task,
    take_snapshot_task,
    validate_table_task,
    wait_children_task,
)
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.extract import ExtractOptions
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.parallel import TableRunContext, parse_child_context
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.plan import TablePlan


def _extract_load_validate(table_plan: TablePlan, ctx: TableRunContext) -> int:
    """Extrai, carrega e valida uma tabela; o mesmo caminho serve ao modo sequencial e ao filho."""
    extracted = extract_table_task(
        infisical_secret_path=ctx.infisical_secret_path,
        project=ctx.project,
        bucket=ctx.bucket,
        table_plan=table_plan,
        snapshot=ctx.snapshot,
        options=ctx.options,
        run_id=ctx.run_id,
    )
    loaded_rows = load_table_task(
        project=ctx.project, dataset_id=ctx.dataset_id, bucket=ctx.bucket, table_plan=table_plan, extracted=extracted
    )
    return validate_table_task(
        project=ctx.project,
        dataset_id=ctx.dataset_id,
        bucket=ctx.bucket,
        table_plan=table_plan,
        extracted=extracted,
        snapshot=ctx.snapshot,
        loaded_rows=loaded_rows,
    )


def _run_child(ctx: TableRunContext, source_schema: str | None, table_id: str) -> None:
    """Modo filho: uma tabela, com a foto e o run id do pai; marca a temporária e nunca publica."""
    table_plan = plan_table_task(
        infisical_secret_path=ctx.infisical_secret_path,
        source_schema=source_schema,
        project=ctx.project,
        dataset_id=ctx.dataset_id,
        table_id=table_id,
        snapshot=ctx.snapshot,
    )
    succeeded = False
    try:
        check_memory_budget_task(plans=[table_plan], options=ctx.options)
        rows = _extract_load_validate(table_plan, ctx)
        stamp_validated_task(
            project=ctx.project,
            dataset_id=ctx.dataset_id,
            bucket=ctx.bucket,
            table_plan=table_plan,
            run_id=ctx.run_id,
            snapshot=ctx.snapshot,
            rows=rows,
        )
        succeeded = True
    finally:
        cleanup_task(
            project=ctx.project,
            dataset_id=ctx.dataset_id,
            bucket=ctx.bucket,
            plans=[table_plan],
            run_id=ctx.run_id,
            drop_temp=not succeeded,
        )


@flow(log_prints=True)
def rj_smfp__nota_carioca_oracle_to_bq(  # noqa: PLR0913
    project: str = "rj-iplanrio-dia",
    dataset_id: str = "brutos_nota_fiscal_staging",
    table_ids: list[str] | None = None,
    source_schema: str | None = None,
    gcs_bucket: str = "rj-iplanrio-dia-bq-to-oracle",
    infisical_secret_path: str = "/db-oracle-nota-fiscal",
    workers: int = 2,
    chunk_size_blocks: int = 32768,
    batch_rows: int = 50_000,
    worker_memory_mb: int = 640,
    pod_memory_mb: int = 1792,
    progress_interval_seconds: int = 30,
    upload_concurrency: int = 4,
    max_pending_files: int = 4,
    parallel_tables: bool = True,
    table_id: str | None = None,
    scn: int | None = None,
    snapshot_taken_at: str | None = None,
    parent_run_id: str | None = None,
) -> None:
    child = parse_child_context(table_id, scn, snapshot_taken_at, parent_run_id)
    rename_current_flow_run_task(new_name=dataset_id if child is None else f"{dataset_id}-{child.table_id}")
    inject_bd_credentials_task(environment="prod")
    options = ExtractOptions(
        workers=workers,
        chunk_size_blocks=chunk_size_blocks,
        batch_rows=batch_rows,
        worker_memory_mb=worker_memory_mb,
        pod_memory_mb=pod_memory_mb,
        progress_interval_seconds=progress_interval_seconds,
        upload_concurrency=upload_concurrency,
        max_pending_files=max_pending_files,
    )
    if child is not None:
        ctx = TableRunContext(
            infisical_secret_path, project, dataset_id, gcs_bucket, child.parent_run_id, child.snapshot, options
        )
        _run_child(ctx, source_schema, child.table_id)
        return
    ensure_exclusive_task()
    drop_leftover_chunk_tasks_task(infisical_secret_path=infisical_secret_path)
    snapshot = take_snapshot_task(infisical_secret_path=infisical_secret_path)
    plans = [
        plan_table_task(
            infisical_secret_path=infisical_secret_path,
            source_schema=source_schema,
            project=project,
            dataset_id=dataset_id,
            table_id=name,
            snapshot=snapshot,
        )
        for name in table_ids or DEFAULT_TABLES
    ]
    run_id = str(flow_run.id)
    try:
        options = check_memory_budget_task(plans=plans, options=options)
        if parallel_tables:
            passthrough: dict[str, object] = {
                "project": project,
                "dataset_id": dataset_id,
                "gcs_bucket": gcs_bucket,
                "source_schema": source_schema,
                "infisical_secret_path": infisical_secret_path,
                "workers": workers,
                "chunk_size_blocks": chunk_size_blocks,
                "batch_rows": batch_rows,
                "worker_memory_mb": worker_memory_mb,
                "pod_memory_mb": pod_memory_mb,
                "progress_interval_seconds": progress_interval_seconds,
                "upload_concurrency": upload_concurrency,
                "max_pending_files": max_pending_files,
            }
            children = launch_children_task(
                table_ids=[table_plan.table_id for table_plan in plans], snapshot=snapshot, passthrough=passthrough
            )
            wait_children_task(children=children)
        else:
            ctx = TableRunContext(infisical_secret_path, project, dataset_id, gcs_bucket, run_id, snapshot, options)
            for table_plan in plans:
                rows = _extract_load_validate(table_plan, ctx)
                stamp_validated_task(
                    project=project,
                    dataset_id=dataset_id,
                    bucket=gcs_bucket,
                    table_plan=table_plan,
                    run_id=run_id,
                    snapshot=snapshot,
                    rows=rows,
                )
        publish_tables_task(project=project, dataset_id=dataset_id, validated=plans, run_id=run_id, snapshot=snapshot)
    finally:
        cleanup_task(project=project, dataset_id=dataset_id, bucket=gcs_bucket, plans=plans, run_id=run_id)
