"""Tasks da paridade F1: dbt pelo deployment compartilhado, leitura do resumo e publicação do relatório."""

from datetime import datetime

from iplanrio.pipelines_utils.logging import log
from prefect import task
from prefect.artifacts import create_markdown_artifact
from prefect.cache_policies import NO_CACHE

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils import dbt, runs
from pipelines.rj_smfp__nota_carioca_paridade_f1.constants import DBT_POLL_SECONDS, REPORT_ARTIFACT_KEY
from pipelines.rj_smfp__nota_carioca_paridade_f1.utils import resumo
from pipelines.rj_smfp__nota_carioca_paridade_f1.utils.paridade import ParidadeReport
from pipelines.rj_smfp__nota_carioca_paridade_f1.utils.render import render_markdown


@task(cache_policy=NO_CACHE)
def run_dbt_task(deployment: str, parameters: dict[str, object], timeout_minutes: int) -> datetime:
    """Roda o deployment de dbt como subflow, espera terminar com sucesso e retorna o fim dele (UTC)."""
    run = runs.start_deployment_run(deployment=deployment, parameters=parameters)
    link = f" ({run.url})" if run.url else ""
    log(f"dbt iniciado: run {run.name} [{run.id}] do deployment {deployment}{link}")
    waiter = dbt.RunWaiter(
        runs=runs.PrefectRuns(),
        timeout_seconds=timeout_minutes * 60,
        poll_seconds=DBT_POLL_SECONDS,
        report=log,
    )
    finished_at = waiter.wait(run.id)
    log(f"dbt concluído às {finished_at:%Y-%m-%d %H:%M:%S} UTC")
    return finished_at


@task(cache_policy=NO_CACHE)
def read_resumo_task(project: str, table: str) -> list[dict[str, object]]:
    """Lê as linhas do resumo de paridade no BigQuery."""
    rows = resumo.BigQueryResumoReader(project, table).read()
    log(f"Resumo de paridade: {len(rows)} linhas em {table}")
    return rows


@task(cache_policy=NO_CACHE)
def publish_report_task(report: ParidadeReport) -> None:
    """Grava o relatório JSON no log e como artefato Markdown do flow run."""
    log(report.to_json())
    create_markdown_artifact(key=REPORT_ARTIFACT_KEY, markdown=render_markdown(report))
