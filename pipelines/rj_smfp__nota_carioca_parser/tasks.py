"""Tasks do parser: preparar as tabelas e processar o índice por NSU."""

from dataclasses import asdict
from datetime import UTC, datetime

from google.cloud import bigquery, storage
from prefect import task
from prefect.cache_policies import NO_CACHE

from pipelines.rj_smfp__nota_carioca_parser.utils.bigquery import ArmazemBigQuery
from pipelines.rj_smfp__nota_carioca_parser.utils.gcs import LeitorGcs
from pipelines.rj_smfp__nota_carioca_parser.utils.incremental import ConfigIncremental, executar_incremental


@task(cache_policy=NO_CACHE)
def garantir_tabelas_task(project_id: str, dataset_id: str, ambiente: str, desde: str) -> None:
    """Cria as tabelas raw, a marca e a quarentena no dataset, se ainda não existirem."""
    ArmazemBigQuery(bigquery.Client(project=project_id), project_id, dataset_id, ambiente, desde).garantir_tabelas()


@task(cache_policy=NO_CACHE)
def processar_incremental_task(  # noqa: PLR0913, PLR0917
    project_id: str,
    dataset_id: str,
    ambiente: str,
    desde: str,
    versao_parser: str,
    nsus_por_lote: int,
    max_lotes: int | None,
    reavaliar_quarentena: bool,
    quarentena_por_execucao: int,
) -> dict[str, object]:
    """Reavalia a quarentena (devolução) e processa o índice acima da marca; devolve o resumo da execução."""
    armazem = ArmazemBigQuery(bigquery.Client(project=project_id), project_id, dataset_id, ambiente, desde)
    leitor = LeitorGcs(storage.Client(project=project_id))
    config = ConfigIncremental(
        versao_parser=versao_parser,
        nsus_por_lote=nsus_por_lote,
        max_lotes=max_lotes,
        reavaliar_quarentena=reavaliar_quarentena,
        quarentena_por_execucao=quarentena_por_execucao,
    )
    return asdict(executar_incremental(armazem, leitor, config, lambda: datetime.now(UTC)))
