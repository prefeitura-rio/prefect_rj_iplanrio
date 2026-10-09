"""Flow for rj_smfp__nota_carioca_oracle_to_bq.

Dois modos no mesmo deployment. O pai (``table_id`` nulo) tira a foto, planeja, confere a memória e, com
``parallel_tables``, lança um filho por tabela, cada um no seu pod, e só publica depois que todos validaram.
O filho (``table_id`` preenchido) extrai, carrega e valida uma tabela e nunca toca nas finais. Com
``parallel_tables=False`` o pai faz tudo em sequência no próprio pod.

Os padrões de memória (``workers=2``, ``worker_memory_mb=640``, ``pod_memory_mb=1792``) são só os do código:
``pod_memory_mb`` é o orçamento do pod, que deve ficar abaixo do ``memory_request`` do deployment (``job_variables``)
menos uma folga, e o deployment passa os dois juntos. Passar do request deixa o scheduler superalocar o nó, que ficou
NotReady.
A publicação é tudo ou nada: todas as tabelas são conferidas antes do primeiro copy e, se um copy falhar, as já
publicadas voltam ao estado anterior por time travel.
"""

from dataclasses import dataclass

from prefect import flow
from prefect.runtime import flow_run

from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.constants import DEFAULT_TABLES
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.table_run import process_table, run_child
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.tasks import (
    check_memory_budget_task,
    cleanup_task,
    drop_leftover_chunk_tasks_task,
    ensure_exclusive_task,
    launch_children_task,
    plan_table_task,
    publish_tables_task,
    take_snapshot_task,
    wait_children_task,
)
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.extract import ExtractOptions
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.notify import NotifierConfig, ParentNotifier
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.notify_view import ParentStage
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.parallel import TableRunContext, parse_child_context
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.plan import TablePlan
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.table_progress import TableReporter, reporting


@dataclass(frozen=True)
class ParentRun:
    """O que o modo pai precisa saber para rodar.

    :param infisical_secret_path: Pasta do segredo do Oracle.
    :param project: Projeto do BigQuery e do GCS.
    :param dataset_id: Dataset de destino.
    :param bucket: Bucket dos Parquet.
    :param run_id: Flow run do pai.
    :param options: Parâmetros de desempenho da extração.
    :param source_schema: Dono das tabelas no Oracle, ou ``None`` para o do segredo.
    :param table_ids: Tabelas a carregar.
    :param parallel_tables: Um filho por tabela (``True``) ou tudo em sequência neste pod.
    :param passthrough: Parâmetros repassados sem mudança aos filhos.
    """

    infisical_secret_path: str
    project: str
    dataset_id: str
    bucket: str
    run_id: str
    options: ExtractOptions
    source_schema: str | None
    table_ids: tuple[str, ...]
    parallel_tables: bool
    passthrough: dict[str, object]


def _extract_tables(run: ParentRun, ctx: TableRunContext, plans: list[TablePlan], notifier: ParentNotifier) -> None:
    """Extrai, carrega e valida todas as tabelas: um filho por tabela, ou em sequência neste pod."""
    if run.parallel_tables:
        children = launch_children_task(
            table_ids=[table_plan.table_id for table_plan in plans], snapshot=ctx.snapshot, passthrough=run.passthrough
        )
        wait_children_task(children=children)
        return
    for table_plan in plans:
        reporter = TableReporter(table_plan.table_id, [notifier.update_table])
        with reporting(reporter), reporter.guard():
            process_table(table_plan, ctx, reporter)


def _run_parent(run: ParentRun, notifier: ParentNotifier) -> None:
    """Modo pai: foto, plano, memória, tabelas (filhos ou sequência), publicação e limpeza, com o Discord."""
    with notifier.guard():
        notifier.begin()
        ensure_exclusive_task()
        drop_leftover_chunk_tasks_task(infisical_secret_path=run.infisical_secret_path)
        snapshot = take_snapshot_task(infisical_secret_path=run.infisical_secret_path)
        notifier.set_snapshot(snapshot)
        notifier.set_stage(ParentStage.PLAN)
        plans = [
            plan_table_task(
                infisical_secret_path=run.infisical_secret_path,
                source_schema=run.source_schema,
                project=run.project,
                dataset_id=run.dataset_id,
                table_id=name,
                snapshot=snapshot,
            )
            for name in run.table_ids
        ]
        try:
            try:
                options = check_memory_budget_task(plans=plans, options=run.options)
                ctx = TableRunContext(
                    run.infisical_secret_path, run.project, run.dataset_id, run.bucket, run.run_id, snapshot, options
                )
                notifier.set_stage(ParentStage.EXTRACTION)
                _extract_tables(run, ctx, plans, notifier)
                notifier.set_stage(ParentStage.PUBLISH)
                publish_tables_task(
                    project=run.project,
                    dataset_id=run.dataset_id,
                    validated=plans,
                    run_id=run.run_id,
                    snapshot=snapshot,
                )
            except BaseException as error:
                notifier.fail(error)
                raise
            notifier.set_stage(ParentStage.CLEANUP)
        finally:
            cleanup_task(
                project=run.project, dataset_id=run.dataset_id, bucket=run.bucket, plans=plans, run_id=run.run_id
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
    discord_notifications: bool = True,
    table_id: str | None = None,
    scn: int | None = None,
    snapshot_taken_at: str | None = None,
    parent_run_id: str | None = None,
) -> None:
    child = parse_child_context(table_id, scn, snapshot_taken_at, parent_run_id)
    run_name = dataset_id if child is None else f"{dataset_id}-{child.table_id}"
    rename_current_flow_run_task(new_name=run_name)
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
        run_child(ctx, source_schema, child.table_id, discord_notifications)
        return
    names = tuple(table_ids or DEFAULT_TABLES)
    run_id = str(flow_run.id)
    notifier = ParentNotifier.create(
        NotifierConfig(dataset_id, run_id, run_name, names, workers, discord_notifications, project, gcs_bucket)
    )
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
        # Os filhos só gravam o JSON de progresso no GCS se o pai for usá-lo; eles nunca postam no Discord.
        "discord_notifications": notifier.enabled,
    }
    _run_parent(
        ParentRun(
            infisical_secret_path,
            project,
            dataset_id,
            gcs_bucket,
            run_id,
            options,
            source_schema,
            names,
            parallel_tables,
            passthrough,
        ),
        notifier,
    )
