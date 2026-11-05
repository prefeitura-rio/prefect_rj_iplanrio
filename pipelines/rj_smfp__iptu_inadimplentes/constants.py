# -*- coding: utf-8 -*-
"""
Constantes e configurações para o pipeline de IPTU Inadimplentes - SMFP.

Este módulo contém todas as configurações de tabelas e queries SQL
para o dump do banco de dados DBINAD (IPTU Inadimplentes).
"""

from dataclasses import dataclass
from enum import Enum


class Constants(Enum):
    """Constantes gerais do pipeline de IPTU Inadimplentes."""

    DATASET_ID = "iptu_inadimplentes"
    DB_DATABASE = "DBINAD"
    DB_HOST = "10.3.23.158"
    DB_PORT = "1433"
    DB_TYPE = "sql_server"
    INFISICAL_SECRET_PATH = "/db-iptu-inadimplentes"


@dataclass
class TableConfig:
    """
    Configuração de uma tabela para dump.

    Attributes:
        table_id: Nome da tabela no BigQuery
        execute_query: Query SQL para extrair os dados
        dump_mode: Modo de dump ('overwrite' ou 'append')
        biglake_table: Se deve criar tabela BigLake
        materialize_after_dump: Se deve materializar após dump
        materialization_mode: Modo de materialização ('prod' ou 'dev')
        materialize_to_datario: Se deve materializar para DataRio
        dump_to_gcs: Se deve fazer dump para GCS
    """

    table_id: str
    execute_query: str
    dump_mode: str = "overwrite"
    biglake_table: bool = True
    materialize_after_dump: bool = True
    materialization_mode: str = "prod"
    materialize_to_datario: bool = False
    dump_to_gcs: bool = False


# Configurações de todas as tabelas do pipeline
TABLE_CONFIGS = {
    "perfil_inadimplente_v2": TableConfig(
        table_id="perfil_inadimplente_v2",
        execute_query="""
            SELECT *
            FROM DBINAD.IPTU.IPTU;
        """,
        dump_mode="overwrite",
        materialize_after_dump=True,
        materialization_mode="prod",
    ),
}
