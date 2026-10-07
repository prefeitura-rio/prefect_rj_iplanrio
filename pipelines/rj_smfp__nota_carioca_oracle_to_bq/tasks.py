"""Tasks da carga Oracle → GCS (Parquet) → BigQuery."""

import time

from prefect import task
from prefect.cache_policies import NO_CACHE
from prefect.runtime import deployment, flow_run
from prefect.settings import PREFECT_UI_URL

from iplanrio.pipelines_utils.logging import log
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.constants import GCS_PREFIX
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils import (
    chunks,
    extract,
    load,
    memory,
    oracle,
    parallel,
    plan,
    publish,
    runs,
)
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.checksum import format_checksums
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.gcs import blob_prefix
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.progress import format_duration, format_size
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.supervise import Supervision, supervise

POLL_SECONDS = 30.0


@task(cache_policy=NO_CACHE)
def ensure_exclusive_task() -> None:
    """Recusa a execução se outra carga deste deployment (pai ou filho) estiver em andamento.

    Não usa limite global de concorrência: um pod morto por OOM deixaria o slot preso.

    :raises ParallelRunError: Se houver outra execução ativa, nomeando-a para o usuário cancelá-la se travada.
    """
    if deployment.id is None:
        log("Execução fora de um deployment; exclusão mútua não verificada")
        return
    conflicts = parallel.find_conflicts(runs.list_active_runs(str(deployment.id)), str(flow_run.id))
    if conflicts:
        raise parallel.ParallelRunError(parallel.describe_conflicts(conflicts))


@task(cache_policy=NO_CACHE)
def drop_leftover_chunk_tasks_task(infisical_secret_path: str) -> None:
    """Apaga as tarefas ``DBMS_PARALLEL_EXECUTE`` deixadas por execuções interrompidas (pods mortos)."""
    dropped = chunks.drop_leftover_chunk_tasks(oracle.read_oracle_config(infisical_secret_path))
    log(f"{len(dropped)} tarefas de chunking deixadas por execuções anteriores apagadas: {dropped}")


@task(cache_policy=NO_CACHE)
def take_snapshot_task(infisical_secret_path: str) -> oracle.Snapshot:
    """Lê o SCN que fixa o ponto de leitura das três tabelas."""
    snapshot = oracle.read_snapshot(oracle.read_oracle_config(infisical_secret_path))
    log(
        f"SCN da foto: {snapshot.scn} ({snapshot.taken_at:%Y-%m-%d %H:%M:%S} UTC); "
        f"sync_id = {snapshot.sync_id}; fonte do SCN: {snapshot.source}"
    )
    return snapshot


@task(cache_policy=NO_CACHE)
def plan_table_task(  # noqa: PLR0913
    infisical_secret_path: str,
    source_schema: str | None,
    project: str,
    dataset_id: str,
    table_id: str,
    snapshot: oracle.Snapshot,
) -> plan.TablePlan:
    """Lê as colunas do Oracle, monta o schema do BigQuery e o confere com a tabela final atual."""
    config = oracle.read_oracle_config(infisical_secret_path)
    schema = oracle.validate_identifier(source_schema or config.schema)
    table_plan = plan.check_destination(
        project, dataset_id, plan.plan_table(config, schema, oracle.validate_identifier(table_id), snapshot)
    )
    log(f"{table_id}: {len(table_plan.columns)} colunas no Oracle; checksum em {list(table_plan.checksum_columns)}")
    if table_plan.changes is None:
        log(
            f"{table_id}: a tabela final ainda não existe em {dataset_id}; "
            f"layout padrão: {table_plan.layout.describe()}"
        )
    else:
        log(f"{table_id}: layout espelhado da tabela final: {table_plan.layout.describe()}")
    if table_plan.changes is not None and table_plan.changes.added:
        log(f"{table_id}: colunas novas aceitas: {list(table_plan.changes.added)}")
    return table_plan


@task(cache_policy=NO_CACHE)
def check_memory_budget_task(plans: list[plan.TablePlan], options: extract.ExtractOptions) -> extract.ExtractOptions:
    """Falha antes de extrair se os workers mais o processo principal não couberem na memória do pod."""
    estimates = {
        table_plan.table_id: memory.plan_worker_memory(
            table_plan.columns, options.worker_memory_mb, options.batch_rows
        ).worker_mb
        for table_plan in plans
    }
    total_mb = memory.check_pod_budget(estimates, options.workers, options.pod_memory_mb)
    log(f"Memória estimada: ~{total_mb} MiB de {options.pod_memory_mb} MiB do pod ({options.workers} workers)")
    return options


@task(cache_policy=NO_CACHE)
def extract_table_task(  # noqa: PLR0913
    infisical_secret_path: str,
    project: str,
    bucket: str,
    table_plan: plan.TablePlan,
    snapshot: oracle.Snapshot,
    options: extract.ExtractOptions,
    run_id: str,
) -> extract.ExtractResult:
    """Lê a tabela em faixas de ROWID com vários processos e grava Parquet no GCS."""
    request = extract.ExtractRequest(
        config=oracle.read_oracle_config(infisical_secret_path),
        schema=table_plan.schema,
        table=table_plan.table_id,
        columns=table_plan.columns,
        checksum_columns=table_plan.checksum_columns,
        snapshot=snapshot,
        project=project,
        bucket=bucket,
        run_id=run_id,
        options=options,
    )
    result = extract.extract_table(request, report=log)
    log(
        f"{result.table}: extração concluída em {format_duration(result.seconds)}: {result.rows:,} linhas, "
        f"{format_size(result.bytes_written)} em {result.files} arquivos ({result.chunks} faixas)"
    )
    log(f"{result.table}: checksums extraídos: {format_checksums(result.checksums)}")
    return result


@task(cache_policy=NO_CACHE)
def load_table_task(
    project: str, dataset_id: str, bucket: str, table_plan: plan.TablePlan, extracted: extract.ExtractResult
) -> int:
    """Carrega os Parquet numa tabela temporária com a mesma partição e cluster da final."""
    started = time.monotonic()
    rows = load.load_table(load.Destination(project, dataset_id, bucket), table_plan, extracted)
    log(
        f"{table_plan.table_id}: {rows:,} linhas carregadas em {dataset_id}.{table_plan.temp_id} "
        f"em {format_duration(time.monotonic() - started)}"
    )
    return rows


@task(cache_policy=NO_CACHE)
def validate_table_task(  # noqa: PLR0913
    infisical_secret_path: str,
    project: str,
    dataset_id: str,
    bucket: str,
    table_plan: plan.TablePlan,
    extracted: extract.ExtractResult,
    snapshot: oracle.Snapshot,
    loaded_rows: int,
) -> int:
    """Compara contagem e checksum do BigQuery com o Oracle no SCN da foto; falha sem tocar na tabela final."""
    started = time.monotonic()
    rows = load.validate_table(
        oracle.read_oracle_config(infisical_secret_path),
        load.Destination(project, dataset_id, bucket),
        table_plan,
        extracted,
        snapshot,
    )
    log(
        f"{table_plan.table_id}: contagem validada, {rows:,} linhas iguais no Oracle (SCN {snapshot.scn}), "
        f"nos arquivos e no BigQuery ({format_duration(time.monotonic() - started)}; carregadas {loaded_rows:,})"
    )
    log(f"{table_plan.table_id}: checksums iguais na extração e no BigQuery: {format_checksums(extracted.checksums)}")
    return rows


@task(cache_policy=NO_CACHE)
def stamp_validated_task(  # noqa: PLR0913
    project: str,
    dataset_id: str,
    bucket: str,
    table_plan: plan.TablePlan,
    run_id: str,
    snapshot: oracle.Snapshot,
    rows: int,
) -> None:
    """Grava na tabela temporária a prova de que foi validada para o pai ``run_id`` e o SCN da foto."""
    load.stamp_validated(load.Destination(project, dataset_id, bucket), table_plan, run_id, snapshot, rows)
    log(f"{table_plan.table_id}: tabela temporária marcada como validada (execução {run_id}, SCN {snapshot.scn})")


@task(cache_policy=NO_CACHE)
def launch_children_task(
    table_ids: list[str], snapshot: oracle.Snapshot, passthrough: dict[str, object]
) -> dict[str, str]:
    """Lança um flow run filho por tabela neste mesmo deployment, em paralelo, cada um no seu pod."""
    if deployment.name is None or flow_run.flow_name is None:
        raise parallel.ParallelRunError("parallel_tables exige execução por um deployment; use parallel_tables=False.")
    run_id = str(flow_run.id)
    parameters = {
        table_id: parallel.build_child_parameters(table_id, snapshot, run_id, passthrough) for table_id in table_ids
    }
    children = runs.launch_children(f"{flow_run.flow_name}/{deployment.name}", parameters)
    ui_url = PREFECT_UI_URL.value()
    for table_id, child_id in children.items():
        link = f" {ui_url}/runs/flow-run/{child_id}" if ui_url else ""
        log(f"{table_id}: filho lançado, flow run {child_id}{link}")
    return children


@task(cache_policy=NO_CACHE)
def wait_children_task(children: dict[str, str]) -> None:
    """Acompanha os filhos a cada 30 s; se algum falhar, cancela os irmãos e falha sem publicar."""
    supervise(
        children, Supervision(read=runs.read_runs, cancel=runs.cancel_runs, report=log, poll_seconds=POLL_SECONDS)
    )


@task(cache_policy=NO_CACHE)
def publish_tables_task(
    project: str, dataset_id: str, validated: list[plan.TablePlan], run_id: str, snapshot: oracle.Snapshot
) -> None:
    """Confere todas as tabelas e as troca tudo ou nada; um copy que falha desfaz os já publicados."""
    started = time.monotonic()
    request = publish.PublishRequest(validated, run_id, snapshot.scn)
    publish.publish_all(publish.BigQueryStore(project, dataset_id), request, log)
    names = [table_plan.table_id for table_plan in validated]
    log(f"Tabelas finais substituídas em {dataset_id}: {names} ({format_duration(time.monotonic() - started)})")


@task(cache_policy=NO_CACHE)
def cleanup_task(  # noqa: PLR0913
    project: str, dataset_id: str, bucket: str, plans: list[plan.TablePlan], run_id: str, drop_temp: bool = True
) -> None:
    """Apaga os arquivos do GCS de ``run_id`` e, com ``drop_temp``, as tabelas temporárias; com sucesso ou falha."""
    prefixes = [blob_prefix(GCS_PREFIX, table_plan.table_id, run_id) for table_plan in plans]
    load.cleanup(load.Destination(project, dataset_id, bucket), plans, prefixes, drop_temp)
    names = [table_plan.table_id for table_plan in plans]
    log(f"Arquivos do GCS apagados{' e tabelas temporárias' if drop_temp else ''} ({names})")
