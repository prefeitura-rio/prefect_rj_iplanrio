from datetime import datetime, timedelta

from prefect.schedules import Interval

from iplanrio.pipelines_utils.prefect import generate_dump_db_schedules
from iplanrio.pipelines_utils.io import untuple_clocks


ergon_queries = [
    {
        "table_id": "fita_banco",
        "materialize_after_dump": True,
        "biglake_table": True,
        "materialization_mode": "prod",
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
        "materialize_after_dump": True,
        "biglake_table": True,
        "materialization_mode": "prod",
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
        "materialize_after_dump": True,
        "biglake_table": True,
        "materialization_mode": "prod",
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


ergon_clocks = generate_dump_db_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2022, 11, 9, 22, 30),
    db_database="P01.PCRJ",
    db_host="10.70.6.21",
    db_port="1526",
    db_type="oracle",
    dataset_id="recursos_humanos_ergon_prefect3",
    infisical_secret_path="db-ergon-prod",
    table_parameters=ergon_queries,
    timezone="America/Sao_Paulo",
)

ergon_daily_schedule = untuple_clocks(ergon_clocks)
