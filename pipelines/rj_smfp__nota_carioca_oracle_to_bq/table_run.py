"""Uma tabela do começo ao fim (extração, carga, validação, marca), no filho ou em sequência no pai, com o andamento."""

from pipelines.rj_smfp__nota_carioca_oracle_to_bq.tasks import (
    check_memory_budget_task,
    cleanup_task,
    extract_table_task,
    load_table_task,
    plan_table_task,
    stamp_validated_task,
    validate_table_task,
)
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.parallel import TableRunContext
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.plan import TablePlan
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.table_progress import (
    ProgressStore,
    TableReporter,
    TableStage,
    reporting,
)


def process_table(table_plan: TablePlan, ctx: TableRunContext, reporter: TableReporter) -> None:
    """Extrai, carrega, valida e marca uma tabela; o mesmo caminho serve ao modo sequencial e ao filho.

    O ``reporter`` só informa o andamento (Discord); as etapas de dados não dependem dele.
    """
    reporter.stage(TableStage.EXTRACTION)
    extracted = extract_table_task(
        infisical_secret_path=ctx.infisical_secret_path,
        project=ctx.project,
        bucket=ctx.bucket,
        table_plan=table_plan,
        snapshot=ctx.snapshot,
        options=ctx.options,
        run_id=ctx.run_id,
    )
    reporter.extracted(extracted)
    loaded_rows = load_table_task(
        project=ctx.project, dataset_id=ctx.dataset_id, bucket=ctx.bucket, table_plan=table_plan, extracted=extracted
    )
    reporter.stage(TableStage.VALIDATION)
    rows = validate_table_task(
        project=ctx.project,
        dataset_id=ctx.dataset_id,
        bucket=ctx.bucket,
        table_plan=table_plan,
        extracted=extracted,
        snapshot=ctx.snapshot,
        loaded_rows=loaded_rows,
    )
    stamp_validated_task(
        project=ctx.project,
        dataset_id=ctx.dataset_id,
        bucket=ctx.bucket,
        table_plan=table_plan,
        run_id=ctx.run_id,
        snapshot=ctx.snapshot,
        rows=rows,
    )
    reporter.stage(TableStage.VALIDATED)


def run_child(ctx: TableRunContext, source_schema: str | None, table_id: str, notify_progress: bool) -> None:
    """Modo filho: uma tabela, com a foto e o run id do pai; marca a temporária e nunca publica.

    Com ``notify_progress`` o filho grava o andamento em ``oracle_to_bq_progress/<run id do pai>/<TABELA>.json`` para o
    pai atualizar o Discord; o filho nunca fala com o Discord. O GCS recebe um JSON pequeno por tick, e a falha da
    gravação é engolida.
    """
    store = ProgressStore(ctx.project, ctx.bucket, ctx.run_id) if notify_progress else None
    reporter = TableReporter(table_id, [store.write] if store is not None else [])
    with reporting(reporter):
        with reporter.guard():
            reporter.stage(TableStage.PLANNING)
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
            with reporter.guard():
                check_memory_budget_task(plans=[table_plan], options=ctx.options)
                process_table(table_plan, ctx, reporter)
            succeeded = True
        finally:
            # O pai lê os JSONs de progresso depois de o filho terminar; só o pai os apaga.
            cleanup_task(
                project=ctx.project,
                dataset_id=ctx.dataset_id,
                bucket=ctx.bucket,
                plans=[table_plan],
                run_id=ctx.run_id,
                drop_temp=not succeeded,
                drop_progress=False,
            )
