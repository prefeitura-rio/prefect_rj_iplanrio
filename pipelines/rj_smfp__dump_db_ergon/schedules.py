# -*- coding: utf-8 -*-

from iplanrio.pipelines_utils.prefect import create_dump_db_schedules


ERGON_QUERIES = [
    {
        "table_id": "IPL_PT_FICHAS",
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
    {
        "table_id": "FICHAS_FINANCEIRAS",
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
        "table_id": "VW_DLK_ERG_FITA_BANCO",
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
]

# General Deployment Settings
BASE_ANCHOR_DATE = "2025-07-15T01:00:00"
BASE_INTERVAL_SECONDS = 3600 * 24  # Run each table every day
RUNS_SEPARATION_MINUTES = 10  # Stagger start times by 10 minutes
TIMEZONE = "America/Sao_Paulo"

# Database & Secret Settings
DB_TYPE = "oracle"
DB_DATABASE = "P01.PCRJ"
DB_HOST = "10.70.6.21"
DB_PORT = 1526
DATASET_ID = "brutos_ergon"
INFISICAL_SECRET_PATH = "/db-ergon-prod"


# --- Generate and Print YAML ---
schedules_config = create_dump_db_schedules(
    table_parameters_list=ERGON_QUERIES,
    base_interval_seconds=BASE_INTERVAL_SECONDS,
    base_anchor_date_str=BASE_ANCHOR_DATE,
    runs_interval_minutes=RUNS_SEPARATION_MINUTES,
    timezone=TIMEZONE,
    db_type=DB_TYPE,
    db_database=DB_DATABASE,
    db_host=DB_HOST,
    db_port=DB_PORT,
    dataset_id=DATASET_ID,
    infisical_secret_path=INFISICAL_SECRET_PATH,
)

# Use sort_keys=False to maintain the intended order of keys in the output
print(schedules_config)
