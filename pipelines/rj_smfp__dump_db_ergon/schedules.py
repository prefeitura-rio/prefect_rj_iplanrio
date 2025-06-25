# -*- coding: utf-8 -*-
from datetime import datetime, timedelta

from iplanrio.pipelines_utils.constants import NOT_SET
from iplanrio.pipelines_utils.io import untuple_clocks
from iplanrio.pipelines_utils.prefect import generate_dump_db_schedules

ergon_queries = [
    {
        "table_id": "fita_banco",
        "biglake_table": True,
        "partition_columns": "MES_ANO",
        "dump_mode": "append",
        "break_query_frequency": "month",
        "break_query_start": "current_month",
        "break_query_end": "current_month",
        "execute_query": """
            SELECT
                LANCAMENTO, NUMFUNC, NUMVINC, MES_ANO, NUMERO, RUBRICA, SETOR,
                VALORVAN, VALORDES, NUMPENS, NUMDEPEN, AGENCIA, BANCO, CONTA, VALORLIQ, TIPOPAG,
                CARGO, REFERENCIA, CENTRO_CUSTO, FUNCAO, NOME, SINDICATO, LOTE, EMP_CODIGO, FICHA,
                REGIMEJUR, TIPOVINC, DTEXERC, DTAPOSENT, DTVAC, CATEGORIA, SUBEMP_CODIGO, DATA_CREDITO,
                NUMREP, EMP_CODIGO_VINC, JORNADA, SUBCATEGORIA, CPF, SUBEMP_CODIGO_GFIP, FLEX_CAMPO_01,
                FLEX_CAMPO_02, FLEX_CAMPO_05, NUMJUR, ID_PESSOA, TIPO_PESSOA, SUB_CC, NOME_REP, CPF_REP,
                TIPOPAG_REP, BANCO_REP, AGENCIA_REP, CONTA_REP
            FROM C_ERGON.VW_DLK_ERG_FITA_BANCO
        """,
    },
    {
        "table_id": "ficha_financeira",
        "biglake_table": True,
        "dump_mode": "append",
        "break_query_frequency": "month",
        "break_query_start": "current_month",
        "break_query_end": "current_month",
        "partition_columns": "MES_ANO_FOLHA",
        "execute_query": """
            SELECT
                MES_ANO_FOLHA,NUM_FOLHA,LANCAMENTO,NUMFUNC,NUMVINC,NUMPENS,MES_ANO_DIREITO,
                RUBRICA,TIPO_RUBRICA,DESC_VANT,COMPLEMENTO,VALOR,CORRECAO,EXECUCAO,EMP_CODIGO
            FROM ERGON.FICHAS_FINANCEIRAS
        """,
    },
    {
        "table_id": "ficha_financeira_contabil",
        "biglake_table": True,
        "dump_mode": "append",
        "break_query_frequency": "month",
        "break_query_start": "current_month",
        "break_query_end": "current_month",
        "partition_columns": "MES_ANO_FOLHA",
        "execute_query": """
        SELECT
            MES_ANO_FOLHA,NUM_FOLHA,NUMFUNC,NUMVINC,NUMPENS,SETOR,SECRETARIA,TIPO_FUNC,
            ATI_INAT_PENS,DETALHA,RUBRICA,TIPO_RUBRICA,MES_ANO_DIREITO,DESC_VANT,VALOR,COMPLEMENTO,
            TIPO_CLASSIF,CLASSIFICACAO,TIPO_CLASSIF_FR,CLASSIF_FR,ELEMDESP,TIPORUB,EMP_CODIGO
        FROM ERGON.IPL_PT_FICHAS
        """,
    },
]


# Generate schedules using the utility function
ergon_clocks = generate_dump_db_schedules(
    interval=timedelta(minutes=5),
    start_date=datetime(2025, 5, 27, 10, 30),
    db_database="P01.PCRJ",
    db_host="10.70.6.21",
    db_port="1526",
    db_type="oracle",
    db_charset=NOT_SET,
    dataset_id="recursos_humanos_ergon_prefect3",
    infisical_secret_path="db-ergon-prod",
    table_parameters=ergon_queries,
    timezone="America/Sao_Paulo",
)

# # Convert the clocks to a format that can be used by Prefect
ergon_daily_schedules = untuple_clocks(ergon_clocks)
# print(ergon_daily_schedules)


# {
#     "batch_size": 50000,
#     "infisical_secret_path": "db-ergon-prod",
#     "db_database": "P01.PCRJ",
#     "db_host": "10.70.6.21",
#     "db_port": "1526",
#     "db_type": "oracle",
#     "dataset_id": "recursos_humanos_ergon_prefect3",
#     "table_id": "ficha_financeira",
#     "db_charset": "NOT_SET",
#     "biglake_table": true,
#     "dump_mode": "append",
#     "execute_query": "\n            SELECT\n                MES_ANO_FOLHA,NUM_FOLHA,LANCAMENTO,NUMFUNC,NUMVINC,NUMPENS,MES_ANO_DIREITO,\n                RUBRICA,TIPO_RUBRICA,DESC_VANT,COMPLEMENTO,VALOR,CORRECAO,EXECUCAO,EMP_CODIGO\n            FROM ERGON.FICHAS_FINANCEIRAS\n        ",
#     "break_query_frequency": "month",
#     "break_query_start": "2000-01-01",
#     "break_query_end": "current_month",
#     "partition_columns": "MES_ANO_FOLHA",
#     "retry_dump_upload_attempts": 1,
#     "batch_data_type": "csv",
#     "log_number_of_batches": 100,
#     "partition_date_format": "%Y-%m-%d",
#     "lower_bound_date": null
# }


# {
#   "db_host": "10.70.6.21",
#   "db_port": "1526",
#   "db_type": "oracle",
#   "table_id": "ficha_financeira",
#   "dump_mode": "append",
#   "batch_size": 50000,
#   "dataset_id": "recursos_humanos_ergon_prefect3",
#   "db_charset": "NOT_SET",
#   "db_database": "P01.PCRJ",
#   "biglake_table": true,
#   "execute_query": "\n            SELECT\n                MES_ANO_FOLHA,NUM_FOLHA,LANCAMENTO,NUMFUNC,NUMVINC,NUMPENS,MES_ANO_DIREITO,\n                RUBRICA,TIPO_RUBRICA,DESC_VANT,COMPLEMENTO,VALOR,CORRECAO,EXECUCAO,EMP_CODIGO\n            FROM ERGON.FICHAS_FINANCEIRAS\n        ",
#   "batch_data_type": "csv",
#   "break_query_end": "current_month",
#   "lower_bound_date": null,
#   "break_query_start": "2000-01-01",
#   "partition_columns": "MES_ANO_FOLHA",
#   "break_query_frequency": "month",
#   "infisical_secret_path": "db-ergon-prod",
#   "log_number_of_batches": 100,
#   "partition_date_format": "%Y-%m-%d",
#   "retry_dump_upload_attempts": 1
# }
