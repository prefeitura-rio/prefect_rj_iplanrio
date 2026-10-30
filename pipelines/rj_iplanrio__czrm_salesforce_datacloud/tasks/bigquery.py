"""BigQuery: garantir dataset/tabelas, carregar staging e fazer o MERGE.

O dataset é criado aqui na primeira execução (``exists_ok``), com
``location="US"`` — o perfil do dbt roda em US e um dataset em outra região
quebraria as queries. Se o time de dados já tiver criado o dataset, a chamada
é um no-op.

A staging é descartável: quando o conjunto de colunas muda (campo novo ou
removido na DLO), ela é recriada. A tabela final só recebe ALTER ADD COLUMN,
nunca DROP — histórico não se perde por causa de drift.
"""

from __future__ import annotations

import pandas as pd
from google.cloud import bigquery
from prefect import task

from pipelines.rj_iplanrio__czrm_salesforce_datacloud.utils.log import logger_da_pipeline
from pipelines.rj_iplanrio__czrm_salesforce_datacloud.utils.queries import ler_query
from pipelines.rj_iplanrio__czrm_salesforce_datacloud.utils.schema import SchemaTabela

logger = logger_da_pipeline(__name__)

LOCATION = "US"
LABELS = {"pipeline": "rj_iplanrio__czrm_salesforce_datacloud"}


def _cliente(project_id: str) -> bigquery.Client:
    return bigquery.Client(project=project_id)


def _full(project_id: str, dataset_id: str, table_id: str) -> str:
    return f"{project_id}.{dataset_id}.{table_id}"


def _assinar(campos: list[bigquery.SchemaField]) -> list[tuple[str, str]]:
    return [(c.name, c.field_type) for c in campos]


def _alinhar(
    client: bigquery.Client,
    tabela: bigquery.Table,
    campos: list[bigquery.SchemaField],
    recriar: bool,
) -> None:
    """Ajusta a tabela existente ao schema atual.

    :param client: cliente do BigQuery.
    :param tabela: tabela existente (já com schema carregado).
    :param campos: schema esperado.
    :param recriar: quando ``True`` (staging), recria se o conjunto de colunas
        ou tipos divergir. Quando ``False`` (final), só adiciona colunas.
    """
    atual = _assinar(list(tabela.schema))
    esperado = _assinar(campos)

    if recriar and atual != esperado:
        logger.info("[BQ] %s: schema mudou — recriando staging", tabela.table_id)
        client.delete_table(tabela.reference, not_found_ok=True)
        client.create_table(bigquery.Table(tabela.reference, schema=campos))
        return

    nomes = {nome for nome, _ in atual}
    novas = [c for c in campos if c.name not in nomes]
    if novas:
        tabela.schema = [*tabela.schema, *novas]
        client.update_table(tabela, ["schema"])
        logger.info("[BQ] %s: colunas adicionadas %s", tabela.table_id, [c.name for c in novas])

    tipos_atuais = dict(atual)
    mudados = [c.name for c in campos if c.name in tipos_atuais and tipos_atuais[c.name] != c.field_type]
    if mudados:
        logger.warning("[BQ] %s: tipo diferente do esperado em %s (não alterado)", tabela.table_id, mudados)


@task(log_prints=True)
def garantir_tabelas(
    project_id: str,
    dataset_id: str,
    schemas: dict[str, SchemaTabela],
    clusters: dict[str, list[str]],
) -> None:
    """Cria (se preciso) o dataset, as stags e as tabelas finais.

    A tabela final é particionada por ``data_criacao`` (dia de criação do
    registro em SP) e clusterizada conforme ``tabelas.yaml``.

    :param project_id: projeto GCP.
    :param dataset_id: dataset de destino.
    :param schemas: mapa tabela -> schema descoberto.
    :param clusters: mapa tabela -> campos de clustering.
    """
    client = _cliente(project_id)
    dataset_ref = bigquery.DatasetReference(project_id, dataset_id)

    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = LOCATION
    dataset.labels = LABELS
    client.create_dataset(dataset, exists_ok=True)

    for nome, schema in schemas.items():
        staging_ref = dataset_ref.table(f"{nome}_staging")
        staging = client.create_table(bigquery.Table(staging_ref, schema=list(schema.campos)), exists_ok=True)
        _alinhar(client, staging, list(schema.campos), recriar=True)

        final = bigquery.Table(dataset_ref.table(nome), schema=schema.campos_finais)
        final.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="data_criacao",
        )
        if clusters.get(nome):
            final.clustering_fields = clusters[nome]
        existente = client.create_table(final, exists_ok=True)
        _alinhar(client, existente, schema.campos_finais, recriar=False)


@task(log_prints=True, retries=3, retry_delay_seconds=[30, 60, 120])
def carregar_staging(
    df: pd.DataFrame,
    project_id: str,
    dataset_id: str,
    staging_id: str,
    campos: list[bigquery.SchemaField],
) -> int:
    """Carrega o DataFrame na staging (append).

    :param df: DataFrame já transformado, na ordem do schema.
    :param project_id: projeto GCP.
    :param dataset_id: dataset de destino.
    :param staging_id: nome da tabela de staging.
    :param campos: schema da staging.
    :returns: número de linhas carregadas.
    """
    if df.empty:
        return 0

    client = _cliente(project_id)
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=campos,
    )
    job = client.load_table_from_dataframe(
        df,
        _full(project_id, dataset_id, staging_id),
        job_config=job_config,
    )
    job.result()
    if job.errors:
        raise RuntimeError(f"[BQ] carga da staging '{staging_id}' com erros: {job.errors}")
    return len(df)


@task(log_prints=True, retries=2, retry_delay_seconds=60)
def merge_staging(  # noqa: PLR0913 - parâmetros identificam o alvo do MERGE
    project_id: str,
    dataset_id: str,
    staging_id: str,
    target_id: str,
    primary_key: str,
    imutaveis: tuple[str, ...] = ("data_criacao",),
) -> dict[str, int]:
    """Faz o MERGE da staging na tabela final e limpa a staging.

    :param project_id: projeto GCP.
    :param dataset_id: dataset.
    :param staging_id: tabela de staging (fonte).
    :param target_id: tabela final (destino).
    :param primary_key: chave do MERGE.
    :param imutaveis: colunas que entram no INSERT mas nunca no UPDATE —
        ``data_criacao`` é a partição e não muda; atualizá-la só gastaria
        reescrita de partição.
    :returns: ``{"inseridas": n, "atualizadas": n}`` do ``dml_stats``.
    """
    client = _cliente(project_id)
    staging_full = _full(project_id, dataset_id, staging_id)
    target_full = _full(project_id, dataset_id, target_id)

    staging_ref = client.get_table(staging_full)
    todas = [f.name for f in staging_ref.schema if f.name != primary_key]
    colunas = [c for c in todas if c not in imutaveis]

    sql = ler_query(
        "merge_staging",
        target=target_full,
        staging=staging_full,
        primary_key=primary_key,
        set_clause=", ".join(f"t.{c} = s.{c}" for c in colunas),
        insert_cols=", ".join([primary_key, *todas]),
        insert_vals=", ".join([f"s.{primary_key}", *(f"s.{c}" for c in todas)]),
    )
    job = client.query(sql)
    job.result()
    stats = job.dml_stats

    client.query(ler_query("truncate_staging", staging=staging_full)).result()

    return {
        "inseridas": (stats.inserted_row_count if stats else 0) or 0,
        "atualizadas": (stats.updated_row_count if stats else 0) or 0,
    }
