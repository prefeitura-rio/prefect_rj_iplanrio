# -*- coding: utf-8 -*-
"""
Constantes e configurações para o pipeline de Atividade Econômica - SMFP.

Este módulo contém todas as configurações de tabelas e queries SQL
para o dump do banco de dados DW_BI_ALVARAS.
"""

from dataclasses import dataclass
from enum import Enum


class Constants(Enum):
    """Constantes gerais do pipeline de Atividade Econômica."""

    DATASET_ID = "atividade_economica"
    DB_DATABASE = "DW_BI_ALVARAS"
    DB_HOST = "10.70.15.11"
    DB_PORT = "1433"
    DB_TYPE = "sql_server"
    INFISICAL_SECRET_PATH = "/db-alvaras"


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
    "fact_fatoalvaras": TableConfig(
        table_id="fact_fatoalvaras",
        execute_query="""
            SELECT
                ID_Alvara,
                Quantidade,
                ID_AtvProcesso,
                ID_CAE,
                ID_CNAE,
                ID_DiaDeferimento,
                ID_DiaSolicitacao,
                ID_DiaTaxaPagamen,
                ID_Direcionamento,
                ID_TipoContribuint,
                ID_TipoSolicitacao
            FROM DW_BI_ALVARAS.dbo.FACT_FatoAlvaras;
        """,
    ),
    "fact_fatocp": TableConfig(
        table_id="fact_fatocp",
        execute_query="""
            SELECT
                ID_AtvProcesso,
                Quantidade_cp,
                ID_CAE,
                ID_CNAE,
                ID_Consulta,
                ID_DiaInicial,
                ID_Direcionamento,
                ID_TipoContribuint,
                ID_TipoSolicitacao
            FROM DW_BI_ALVARAS.dbo.FACT_FatoCP;
        """,
    ),
    "tab_alvara": TableConfig(
        table_id="tab_alvara",
        execute_query="""
            SELECT
                ID_Alvara,
                DSC_Alvara,
                DSC_Endereco,
                DSC_Bairro,
                DSC_Zoneamento,
                DSC_IRLF,
                DSC_TipoAnalise,
                DSC_TempoRespDia,
                DSC_StatusIntermediario,
                DSC_StatusCPL,
                DSC_TempoRespMinuto,
                DSC_TipoAlvara,
                DSC_TaxaOriginal,
                DSC_TaxaMulta,
                DSC_TaxaMora,
                DSC_TaxaTotal,
                DSC_IsentoTaxa,
                DSC_Numero,
                DSC_AlvaraLiberado
            FROM DW_BI_ALVARAS.dbo.TAB_ALVARA;
        """,
    ),
    "tab_atvprocesso": TableConfig(
        table_id="tab_atvprocesso",
        execute_query="""
            SELECT
                ID_AtvProcesso,
                DSC_AtvProcesso,
                DSC_RespAtividade,
                DSC_RefAtividade
            FROM DW_BI_ALVARAS.dbo.TAB_AtvProcesso;
        """,
    ),
    "tab_cae": TableConfig(
        table_id="tab_cae",
        execute_query="""
            SELECT
                ID_CAE,
                DSC_CAE,
                ID_TipoAtividade,
                DSC_TipoAtividade
            FROM DW_BI_ALVARAS.dbo.TAB_CAE;
        """,
    ),
    "tab_cnae": TableConfig(
        table_id="tab_cnae",
        execute_query="""
            SELECT
                ID_CNAE,
                DSC_CNAE
            FROM DW_BI_ALVARAS.dbo.TAB_CNAE;
        """,
    ),
    "tab_consulta": TableConfig(
        table_id="tab_consulta",
        execute_query="""
            SELECT
                ID_Consulta,
                DSC_Consulta,
                DSC_Endereco_cp,
                DSC_Bairro_cp,
                DSC_Zoneamento_cp,
                DSC_CodeConsulta,
                DSC_IRLF_cp,
                DSC_StatusCPL_cp,
                DSC_TipoAnalise_cp,
                DSC_Status_cp
            FROM DW_BI_ALVARAS.dbo.TAB_Consulta;
        """,
    ),
    "tab_direcionamento": TableConfig(
        table_id="tab_direcionamento",
        execute_query="""
            SELECT
                ID_Direcionamento,
                DSC_Direcionamento
            FROM DW_BI_ALVARAS.dbo.TAB_Direcionamento;
        """,
    ),
    "tab_tipocontribuinte_tipocontribuint": TableConfig(
        table_id="tab_tipocontribuinte_tipocontribuint",
        execute_query="""
            SELECT
                ID_TipoContribuint,
                DSC_TipoContribuint
            FROM DW_BI_ALVARAS.dbo.TAB_TipoContribuinte_TipoContribuint;
        """,
    ),
    "tab_tiposolicitacao": TableConfig(
        table_id="tab_tiposolicitacao",
        execute_query="""
            SELECT
                ID_TipoSolicitacao,
                DSC_TipoSolicitacao
            FROM DW_BI_ALVARAS.dbo.TAB_TipoSolicitacao;
        """,
    ),
}
