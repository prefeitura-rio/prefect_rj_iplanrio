"""Valida e publica o scoring do dia — truncate atômico (1 load job) na tabela de destino.

Um load job de ``WRITE_TRUNCATE`` é atômico no BigQuery: quem consulta a tabela enquanto o
load roda ainda vê os dados de ontem, nunca uma tabela vazia ou pela metade. Se a validação
falhar antes do load, a tabela de ontem fica intacta e o flow levanta erro (sem publicar).
"""

from pathlib import Path

from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from iplanrio.pipelines_utils.env import get_bd_credentials_from_env
from prefect import task
from prefect_rj_iplanrio.logging import get_logger

from pipelines.rj_crm__modelo_qualidade_telefone import constants
from pipelines.rj_crm__modelo_qualidade_telefone.tasks.calcula_scores import EstatisticasScores

logger = get_logger(__name__)

# Quanto o volume de linhas pode variar (pra mais ou pra menos) em relação à rodada
# anterior antes de recusar publicar — protege contra uma fonte quebrada silenciosamente
# (ex.: JOIN que zera o universo) sem exigir um número exato, que varia dia a dia.
TOLERANCIA_VARIACAO_LINHAS = 0.3


def linhas_da_tabela_atual(client: bigquery.Client, dataset_id: str, table_id: str) -> int | None:
    """Linhas da tabela antes desta rodada (antes do truncate).

    :param client: Cliente do BigQuery.
    :param dataset_id: Dataset da tabela.
    :param table_id: Nome da tabela.
    :returns: A contagem, ou ``None`` se a tabela ainda não existir (1ª execução).
    """
    try:
        return client.get_table(f"{client.project}.{dataset_id}.{table_id}").num_rows
    except NotFound:
        return None


def valida_scores(stats: EstatisticasScores, n_linhas_anterior: int | None) -> None:
    """Recusa publicar um scoring claramente quebrado.

    :param stats: Estatísticas da rodada (ver ``calcula_scores.gera_parquet_scores``).
    :param n_linhas_anterior: Linhas da tabela antes desta rodada, ou ``None`` na 1ª execução.
    :raises ValueError: Se não houver nenhuma linha, houver probabilidade inválida, ou o
        volume variar mais que :data:`TOLERANCIA_VARIACAO_LINHAS` em relação à rodada anterior.
    """
    if stats.n_linhas == 0:
        raise ValueError("scoring sem nenhuma linha — não publica")
    if stats.n_probs_invalidas > 0:
        raise ValueError(f"{stats.n_probs_invalidas} probabilidade(s) inválida(s) (fora de [0, 1]) — não publica")
    if n_linhas_anterior:
        variacao = abs(stats.n_linhas - n_linhas_anterior) / n_linhas_anterior
        if variacao > TOLERANCIA_VARIACAO_LINHAS:
            raise ValueError(
                f"volume de linhas variou {variacao:.0%} em relação à rodada anterior "
                f"({n_linhas_anterior:,} -> {stats.n_linhas:,}), acima da tolerância "
                f"de {TOLERANCIA_VARIACAO_LINHAS:.0%} — não publica"
            )


def publica_parquet(client: bigquery.Client, caminho: Path, dataset_id: str, table_id: str) -> int:
    """Sobrescreve a tabela inteira com o conteúdo do parquet, num load job atômico.

    O schema (incluindo os STRUCTs ``features``/``shap``) vem do próprio parquet, não é
    declarado aqui — evita duas fontes de verdade com ``calcula_scores.monta_tabela``.

    :param client: Cliente do BigQuery.
    :param caminho: Parquet gerado por ``calcula_scores.gera_parquet_scores``.
    :param dataset_id: Dataset de destino.
    :param table_id: Tabela de destino.
    :returns: Linhas carregadas.
    """
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET, write_disposition="WRITE_TRUNCATE"
    )
    with caminho.open("rb") as arquivo:
        job = client.load_table_from_file(
            arquivo, f"{client.project}.{dataset_id}.{table_id}", job_config=job_config
        )
    job.result()
    logger.info("%s.%s publicada: %d linhas", dataset_id, table_id, job.output_rows)
    return job.output_rows


@task
def publica_scores_task(
    caminho_parquet: Path,
    stats: EstatisticasScores,
    environment: str,
    dataset_id: str = constants.DATASET_ID,
    table_id: str = constants.TABLE_ID_SCORE,
) -> None:
    """Valida a rodada e publica — ver :func:`valida_scores` e :func:`publica_parquet`.

    :raises ValueError: Ver :func:`valida_scores`.
    """
    credentials = get_bd_credentials_from_env(mode=environment)
    client = bigquery.Client(credentials=credentials, project=constants.PROJECT_ID)

    n_linhas_anterior = linhas_da_tabela_atual(client, dataset_id, table_id)
    valida_scores(stats, n_linhas_anterior)
    publica_parquet(client, caminho_parquet, dataset_id, table_id)
