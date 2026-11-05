"""Tasks da sonda de leitura do Oracle da Nota Carioca."""

from dataclasses import replace

from prefect import task
from prefect.artifacts import create_markdown_artifact
from prefect.cache_policies import NO_CACHE
from prefect.runtime import flow_run

from iplanrio.pipelines_utils.logging import log
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.oracle import read_oracle_config, validate_identifier
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.database import DatabaseInfo, collect_database
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.environment import EnvironmentInfo, describe_environment
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.gcs import delete_run_prefix
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.measure import TableBenchmark, benchmark_table
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.options import ProbeOptions
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.profile import ProfileRequest, TableProfile, profile_table
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.report_bench import format_benchmark, format_scaling
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.report_setup import (
    format_database,
    format_environment,
    format_profile,
)
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.report_summary import (
    format_extrapolation,
    format_verdict,
    markdown_summary,
)
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.runner import summarize
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.scaling import ScalingRun, scale_table
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.session import Snapshot, flashback_failures, read_snapshot


@task(cache_policy=NO_CACHE)
def build_options_task(  # noqa: PLR0913
    table_ids: list[str],
    source_schema: str | None,
    infisical_secret_path: str,
    chunk_size_blocks: int,
    sample_chunks: int,
    worker_counts: list[int],
    worker_memory_mb: int,
    batch_rows: int,
    test_upload: bool,
    gcs_bucket: str,
    project: str,
    compare_no_scn: bool,
    compression_variants: list[str],
) -> ProbeOptions:
    """Valida os parâmetros e resolve o schema (o do Infisical se não for informado)."""
    config = read_oracle_config(infisical_secret_path)
    return ProbeOptions(
        run_id=str(flow_run.id),
        schema=validate_identifier(source_schema or config.schema),
        tables=tuple(validate_identifier(table) for table in table_ids),
        infisical_secret_path=infisical_secret_path,
        chunk_size_blocks=chunk_size_blocks,
        sample_chunks=sample_chunks,
        worker_counts=tuple(worker_counts),
        worker_memory_mb=worker_memory_mb,
        batch_rows=batch_rows,
        test_upload=test_upload,
        gcs_bucket=gcs_bucket,
        project=project,
        compare_no_scn=compare_no_scn,
        compression_variants=tuple(compression_variants),
    )


@task(cache_policy=NO_CACHE)
def environment_task() -> EnvironmentInfo:
    """Registra o ambiente do pod (seção 1)."""
    info = describe_environment()
    log(format_environment(info))
    return info


@task(cache_policy=NO_CACHE)
def database_task(options: ProbeOptions) -> DatabaseInfo:
    """Registra as informações do banco (seção 2); falta de privilégio não aborta."""
    info = collect_database(read_oracle_config(options.infisical_secret_path))
    log(format_database(info))
    return info


@task(cache_policy=NO_CACHE)
def snapshot_task(options: ProbeOptions) -> Snapshot:
    """Lê o SCN único das leituras e confere se ``AS OF SCN`` funciona em todas as tabelas."""
    config = read_oracle_config(options.infisical_secret_path)
    snapshot = read_snapshot(config)
    log(
        f"Foto de leitura: SCN {snapshot.scn:,} (fonte: {snapshot.source}) em {snapshot.taken_at:%Y-%m-%d %H:%M:%S} UTC"
    )
    failures = flashback_failures(config, options.schema, options.tables, snapshot.scn)
    if not failures:
        log("AS OF SCN conferido em todas as tabelas.")
        return snapshot
    log(
        "AS OF SCN falhou; as medições seguem SEM SCN (a carga real precisa dele). "
        "Peça à DBA: GRANT FLASHBACK ANY TABLE (ou FLASHBACK nas tabelas).\n" + "\n".join(failures),
        level="warning",
    )
    return replace(snapshot, flashback=False)


@task(cache_policy=NO_CACHE)
def profile_table_task(options: ProbeOptions, table: str, block_size: int) -> TableProfile:
    """Perfila a tabela e conta as faixas de ROWID (seção 3)."""
    config = read_oracle_config(options.infisical_secret_path)
    profile = profile_table(ProfileRequest(config=config, options=options, table=table, block_size=block_size))
    log(format_profile(profile, options.schema, options.chunk_size_blocks))
    return profile


@task(cache_policy=NO_CACHE)
def benchmark_table_task(options: ProbeOptions, snapshot: Snapshot, profile: TableProfile) -> TableBenchmark:
    """Mede as etapas em um processo sobre as faixas amostradas (seção 4)."""
    config = read_oracle_config(options.infisical_secret_path)
    benchmark = benchmark_table(config, options, profile, snapshot)
    log(format_benchmark(benchmark))
    return benchmark


@task(cache_policy=NO_CACHE)
def scaling_task(
    options: ProbeOptions, snapshot: Snapshot, profile: TableProfile, pod_memory_mb: int | None
) -> list[ScalingRun]:
    """Mede a escala com vários processos (seção 5)."""
    config = read_oracle_config(options.infisical_secret_path)
    runs = scale_table(config, options, profile, snapshot, pod_memory_mb)
    log(format_scaling(profile.table, runs))
    return runs


@task(cache_policy=NO_CACHE)
def summary_task(
    environment: EnvironmentInfo,
    profiles: list[TableProfile],
    benchmarks: list[TableBenchmark],
    scalings: list[list[ScalingRun]],
) -> None:
    """Registra a extrapolação e o gargalo (seções 6 e 7) e publica o artefato."""
    all_rates, verdicts = summarize(profiles, benchmarks, scalings)
    log(format_extrapolation(all_rates))
    log(format_verdict(benchmarks, {p.table: runs for p, runs in zip(profiles, scalings, strict=True)}, environment))
    create_markdown_artifact(key="oracle-probe-summary", markdown=markdown_summary(all_rates, verdicts))


@task(cache_policy=NO_CACHE)
def cleanup_gcs_task(options: ProbeOptions) -> None:
    """Apaga do GCS tudo o que a sonda gravou neste flow run."""
    if options.test_upload:
        deleted = delete_run_prefix(options.project, options.gcs_bucket, options.run_id)
        log(f"GCS: {deleted} objetos restantes removidos de gs://{options.gcs_bucket}/oracle_probe/{options.run_id}/")
