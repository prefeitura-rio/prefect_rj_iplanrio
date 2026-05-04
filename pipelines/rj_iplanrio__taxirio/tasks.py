# -*- coding: utf-8 -*-
"""
Tasks para dump de collections do MongoDB do TaxiRio - Migrado para Prefect 3.0.

Este módulo contém todas as tasks necessárias para extrair dados do MongoDB
do TaxiRio e preparar para upload no BigQuery.
"""

from collections.abc import Callable
from datetime import datetime, timedelta
from itertools import pairwise
from pathlib import Path
from typing import Any

from prefect import task
from prefeitura_rio.pipelines_utils.infisical import get_secret
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongoarrow.api import Schema, aggregate_arrow_all
from pytz import UTC

from pipelines.rj_iplanrio__taxirio import utils


@task
def get_mongodb_connection_string(secret_name: str, secret_path: str = "/taxirio") -> str:
    """
    Obtém a string de conexão do MongoDB a partir do Infisical.

    Args:
        secret_name: Nome do secret no Infisical
        secret_path: Caminho do secret no Infisical

    Returns:
        String de conexão do MongoDB
    """
    utils.log("Obtendo string de conexão do MongoDB do Infisical")

    connection = get_secret(
        secret_name=secret_name,
        path=secret_path,
    )

    return connection[secret_name]


@task
def get_mongodb_client(connection: str) -> MongoClient:
    """
    Cria um client MongoDB a partir da string de conexão.

    Args:
        connection: String de conexão do MongoDB

    Returns:
        Cliente MongoDB conectado
    """
    utils.log("Criando cliente MongoDB")

    return MongoClient(connection)


@task
def get_mongodb_collection(client: MongoClient, database: str, collection: str) -> Collection:
    """
    Obtém uma collection do MongoDB.

    Args:
        client: Cliente MongoDB
        database: Nome do database
        collection: Nome da collection

    Returns:
        Collection do MongoDB
    """
    utils.log(f"Acessando collection '{collection}' no database '{database}'")

    return client[database][collection]


@task
def get_dates_for_dump_mode(
    dump_mode: str,
    collection: Collection,
    date_field: str = "createdAt",
) -> tuple[datetime, datetime]:
    """
    Calcula as datas de início e fim baseado no modo de dump.

    Para modo 'overwrite', busca as datas mínima e máxima na collection
    com margem de 30 dias. Para modo 'append', usa a data atual com margem de 1 dia.

    Args:
        dump_mode: Modo de dump ('overwrite' ou 'append')
        collection: Collection do MongoDB
        date_field: Campo de data usado para buscar os limites

    Returns:
        Tupla com (data_inicio, data_fim)
    """
    utils.log(f"Calculando datas para dump_mode='{dump_mode}'")

    if dump_mode == "overwrite":
        base_start = utils.get_mongodb_date_in_collection(collection, order=1, date_field=date_field)
        base_end = utils.get_mongodb_date_in_collection(collection, order=-1, date_field=date_field)
        delta = 30
    else:
        base_start = base_end = datetime.now(UTC)
        delta = 1

    start = utils.normalize_date(base_start) - timedelta(days=delta)
    end = utils.normalize_date(base_end) + timedelta(days=delta)

    utils.log(f"Período calculado: {start.strftime('%Y-%m-%d')} até {end.strftime('%Y-%m-%d')}")

    return start, end


@task
def dump_collection_from_mongodb(
    collection: Collection,
    path: str,
    schema: Schema,
    pipeline: list[dict[str, Any]],
    partition_cols: list[str] | None = None,
) -> Path:
    """
    Faz dump de uma collection completa do MongoDB para disco.

    Executa um pipeline de agregação no MongoDB e salva o resultado em formato Parquet.

    Args:
        collection: Collection do MongoDB
        path: Caminho onde salvar os dados
        schema: Schema PyArrow para a collection
        pipeline: Pipeline de agregação do MongoDB
        partition_cols: Colunas para particionamento dos arquivos Parquet

    Returns:
        Caminho raiz onde os dados foram salvos
    """
    utils.log(f"Iniciando dump da collection '{collection.name}' do MongoDB")

    root_path = Path(path)
    root_path.mkdir(exist_ok=True)

    utils.log("Executando agregação no MongoDB")
    data = aggregate_arrow_all(collection, pipeline=pipeline, schema=schema)

    utils.log("Gravando dados em disco")
    utils.write_data_to_disk(data, root_path, collection.name, partition_cols)

    utils.log(f"Dump concluído: {root_path}")

    return root_path


@task
def dump_collection_from_mongodb_per_period(
    collection: Collection,
    path: str,
    generate_pipeline: Callable,
    schema: Schema,
    freq: str,
    start_date: datetime,
    end_date: datetime,
    partition_cols: list[str] | None = None,
) -> Path:
    """
    Faz dump de uma collection do MongoDB dividida por períodos.

    Útil para collections grandes que precisam ser processadas em lotes por
    intervalo de tempo (diário, mensal, etc).

    Args:
        collection: Collection do MongoDB
        path: Caminho onde salvar os dados
        generate_pipeline: Função que gera o pipeline de agregação para cada período
        schema: Schema PyArrow para a collection
        freq: Frequência de divisão ('D' para diário, 'M' para mensal, etc)
        start_date: Data de início do processamento
        end_date: Data de fim do processamento
        partition_cols: Colunas para particionamento dos arquivos Parquet

    Returns:
        Caminho raiz onde os dados foram salvos
    """
    utils.log(f"Iniciando dump por período da collection '{collection.name}' do MongoDB")
    utils.log(f"Período: {start_date.strftime('%Y-%m-%d')} até {end_date.strftime('%Y-%m-%d')}")
    utils.log(f"Frequência: {freq}")

    root_path = Path(path)
    root_path.mkdir(exist_ok=True)

    utils.log("Gerando intervalos de datas")
    dates = utils.get_date_range(
        start=start_date,
        end=end_date,
        freq=freq,
    )

    for start, end in pairwise(dates):
        utils.log(
            f"Processando período: {start.strftime('%Y-%m-%d')} até {end.strftime('%Y-%m-%d')}"
        )

        data = aggregate_arrow_all(
            collection,
            pipeline=generate_pipeline(start, end),
            schema=schema,
        )

        utils.log("Gravando dados em disco")
        utils.write_data_to_disk(data, root_path, collection.name, partition_cols)

    utils.log(f"Dump por período concluído: {root_path}")

    return root_path
