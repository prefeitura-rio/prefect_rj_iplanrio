# -*- coding: utf-8 -*-
"""
Constantes e configurações para o pipeline de Porte de Empresa - SMFP.

Este módulo contém todas as configurações de tabelas e queries SQL
para o dump do banco de dados SDI (Receita Federal - CNPJ).
"""

from dataclasses import dataclass
from enum import Enum


class Constants(Enum):
    """Constantes gerais do pipeline de Porte de Empresa."""

    DATASET_ID = "porte_empresa"
    DB_DATABASE = "SDI"
    DB_HOST = "10.70.1.34"
    DB_PORT = "1433"
    DB_TYPE = "sql_server"
    INFISICAL_SECRET_PATH = "/db-porte-empresa"


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
        partition_columns: Colunas de particionamento
        partition_date_format: Formato de data para particionamento
        break_query_frequency: Frequência de quebra de query
        break_query_start: Data de início para quebra de query
        break_query_end: Data de fim para quebra de query
    """

    table_id: str
    execute_query: str
    dump_mode: str = "overwrite"
    biglake_table: bool = True
    materialize_after_dump: bool = True
    materialization_mode: str = "prod"
    materialize_to_datario: bool = False
    dump_to_gcs: bool = False
    partition_columns: str = None
    partition_date_format: str = "%Y-%m-%d"
    break_query_frequency: str = None
    break_query_start: str = None
    break_query_end: str = None


# Configurações de todas as tabelas do pipeline
TABLE_CONFIGS = {
    "situacao_cadastral": TableConfig(
        table_id="situacao_cadastral",
        execute_query="""
            SELECT
                CNPJ_basico,
                CNPJ_ordem,
                CNPJ_dv,
                RazaoSocial,
                cd_PorteEmpresa,
                cd_SituacaoCadastral,
                dt_SituacaoCadastral
            FROM SDI.ReceitaFederal.Vw_PorteEmpresa_Sigma
        """,
        dump_mode="append",
        partition_columns="dt_SituacaoCadastral",
        partition_date_format="%Y-%m-%d",
        break_query_frequency="month",
        break_query_start="current_month",
        break_query_end="current_month",
    ),
}
