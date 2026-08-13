# -*- coding: utf-8 -*-
"""
Constantes e configurações para o pipeline de Administração de Instrumentos Firmados - SMFP.

Este módulo contém todas as configurações de tabelas e queries SQL
para o dump do banco de dados DW_IGAA (FINCON - Instrumentos Jurídicos).
"""

from dataclasses import dataclass
from enum import Enum


class Constants(Enum):
    """Constantes gerais do pipeline de Administração de Instrumentos Firmados."""

    DATASET_ID = "adm_instrumentos_firmados"
    DB_DATABASE = "DW_IGAA"
    DB_HOST = "10.2.221.17"
    DB_PORT = "1433"
    DB_TYPE = "sql_server"
    INFISICAL_SECRET_PATH = "/db-fincon"


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
    "instrumento_firmado": TableConfig(
        table_id="instrumento_firmado",
        execute_query="""
            SELECT
                Exercicio,
                Numero,
                Orgao_Executor,
                Descricao_Orgao_Executor,
                Objeto,
                Especie,
                Situacao,
                Cnpj,
                Favorecido,
                Processo,
                Data_Inicio_Prev,
                Data_Fim_Prev,
                Valor_Atualizado,
                Valor_Pago,
                Pt,
                Natureza,
                Fonte,
                Modalidade_Licitacao,
                Embasamento_Legal,
                Saldo_Exec,
                Valor_Empenhado,
                Valor_Liquidado,
                Data_Assinatura,
                Tipo_Favorecido
            FROM DW_IGAA.dbo.VINSTRUMENTOS_JURIDICOS_ESCRITORIO_GBP
        """,
        dump_mode="overwrite",
        biglake_table=True,
        materialize_after_dump=True,
        materialization_mode="prod",
    ),
}
