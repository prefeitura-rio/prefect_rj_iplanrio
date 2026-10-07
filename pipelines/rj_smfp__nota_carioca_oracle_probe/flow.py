"""Flow for rj_smfp__nota_carioca_oracle_probe."""

from prefect import flow

from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from pipelines.rj_smfp__nota_carioca_oracle_probe.constants import (
    DEFAULT_COMPRESSIONS,
    DEFAULT_TABLES,
    DEFAULT_WORKER_COUNTS,
)
from pipelines.rj_smfp__nota_carioca_oracle_probe.tasks import (
    benchmark_table_task,
    build_options_task,
    cleanup_gcs_task,
    database_task,
    environment_task,
    profile_table_task,
    scaling_task,
    snapshot_task,
    summary_task,
)


@flow(log_prints=True)
def rj_smfp__nota_carioca_oracle_probe(  # noqa: PLR0913
    table_ids: list[str] | None = None,
    source_schema: str | None = None,
    infisical_secret_path: str = "/db-oracle-nota-fiscal",
    chunk_size_blocks: int = 32768,
    sample_chunks: int = 4,
    worker_counts: list[int] | None = None,
    worker_memory_mb: int = 1536,
    batch_rows: int = 50000,
    test_upload: bool = True,
    gcs_bucket: str = "rj-iplanrio-dia-bq-to-oracle",
    project: str = "rj-iplanrio-dia",
    compare_no_scn: bool = True,
    compression_variants: list[str] | None = None,
) -> None:
    """Mede, somente leitura, a velocidade de extração das tabelas DFEN do Oracle e acha o gargalo.

    :param table_ids: Tabelas a medir; padrão ``DPS``, ``NOTAS_NACIONAIS`` e ``PESSOAS_NACIONAIS``.
    :param source_schema: Schema das tabelas; padrão o ``DB_SCHEMA`` do Infisical.
    :param infisical_secret_path: Pasta do segredo do Oracle no Infisical.
    :param chunk_size_blocks: Tamanho de cada faixa de ROWID, em blocos.
    :param sample_chunks: Faixas medidas por tabela, espalhadas pela lista de faixas.
    :param worker_counts: Números de workers do teste de escala; padrão 1, 2 e 4.
    :param worker_memory_mb: Orçamento de memória de um worker, em MiB.
    :param batch_rows: Teto de linhas por lote; o lote real sai de ``plan_worker_memory``.
    :param test_upload: Se envia o Parquet ao GCS (sob ``oracle_probe/<flow_run_id>/``, sempre apagado).
    :param gcs_bucket: Bucket do teste de envio.
    :param project: Projeto do GCS.
    :param compare_no_scn: Se compara uma faixa com e sem ``AS OF SCN``.
    :param compression_variants: Compressões do Parquet; padrão ``zstd``, ``snappy`` e ``none``.
    """
    rename_current_flow_run_task(new_name="sonda-oracle-nota-carioca")
    inject_bd_credentials_task(environment="prod")
    options = build_options_task(
        table_ids=table_ids or list(DEFAULT_TABLES),
        source_schema=source_schema,
        infisical_secret_path=infisical_secret_path,
        chunk_size_blocks=chunk_size_blocks,
        sample_chunks=sample_chunks,
        worker_counts=worker_counts or list(DEFAULT_WORKER_COUNTS),
        worker_memory_mb=worker_memory_mb,
        batch_rows=batch_rows,
        test_upload=test_upload,
        gcs_bucket=gcs_bucket,
        project=project,
        compare_no_scn=compare_no_scn,
        compression_variants=compression_variants or list(DEFAULT_COMPRESSIONS),
    )
    try:
        environment = environment_task()
        database = database_task(options)
        snapshot = snapshot_task(options)
        profiles = [
            profile_table_task(options=options, table=table, block_size=database.block_size) for table in options.tables
        ]
        benchmarks = [benchmark_table_task(options, snapshot, profile) for profile in profiles]
        scalings = [scaling_task(options, snapshot, profile, environment.memory_limit_mb) for profile in profiles]
        summary_task(environment, profiles, benchmarks, scalings)
    finally:
        cleanup_gcs_task(options)
