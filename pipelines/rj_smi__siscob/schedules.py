# -*- coding: utf-8 -*-

from iplanrio.pipelines_utils.prefect import create_dump_db_schedules

SISCOB_QUERIES = [
    {
        "table_id": "obra",
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT
                DISTINCT
                    CD_OBRA,
                    REPLACE(REPLACE(DS_TITULO, CHAR(13), ''), CHAR(10), '') AS DS_TITULO,
                    ORGAO_CONTRATANTE,
                    ORGAO_EXECUTOR,
                    NR_PROCESSO,
                    OBJETO,
                    NM_FAVORECIDO,
                    CNPJ,
                    NR_LICITACAO,
                    MODALIDADE,
                    DT_ASS_CONTRATO,
                    DT_INICIO_OBRA,
                    DT_TERMINO_PREVISTO,
                    DT_TERMINO_ATUAL,
                    NR_CONTRATO,
                    AA_EXERCICIO,
                    SITUACAO,
                    VL_ORCADO_C_BDI,
                    VL_CONTRATADO,
                    VL_VIGENTE,
                    PC_MEDIDO,
                    PRAZO_INICIAL
            FROM dbo.fuSEGOVI_Dados_da_Obra()
            """,
    },
    {
        "table_id": "medicao",
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT
                DISTINCT
                    CD_OBRA,
                    NR_MEDICAO,
                    CD_ETAPA,
                    TP_MEDICAO_D,
                    DT_INI_MEDICAO,
                    DT_FIM_MEDICAO,
                    VL_FINAL
            FROM dbo.fuSEGOVI_Medicoes()
            """,
    },
    {
        "table_id": "termo_aditivo",
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT
                DISTINCT
                    CD_OBRA,
                    NR_DO_TERMO,
                    TP_ACERTO,
                    DT_DO,
                    DT_AUTORIZACAO,
                    VL_ACERTO
            FROM dbo.fuSEGOVI_Termos_Aditivos()
            """,
    },
    {
        "table_id": "cronograma_financeiro",
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT
                DISTINCT
                    CD_OBRA,
                    ETAPA,
                    DT_INICIO_ETAPA,
                    DT_FIM_ETAPA,
                    PC_PERCENTUAL,
                    VL_ESTIMADO
            FROM dbo.fuSEGOVI_Cronograma_Financeiro()
            """,
    },
    {
        "table_id": "cronograma_alteracao",
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT
                DISTINCT
                    CD_OBRA,
                    NR_PROCESSO,
                    TP_ALTERACAO,
                    DT_PUBL_DO,
                    CD_ETAPA,
                    NR_PRAZO,
                    DT_VALIDADE,
                    REPLACE(REPLACE(DS_OBSERVACAO, CHAR(13), ''), CHAR(10), '') AS DS_OBSERVACAO
            FROM dbo.fuSEGOVI_Alteração_de_Cronograma()
            """,
    },
    {
        "table_id": "obras_suspensas",
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT
                DISTINCT
                    CD_OBRA,
                    REPLACE(REPLACE(DS_TITULO_OBJETO, CHAR(13), ''), CHAR(10), '') AS DS_TITULO_OBJETO,
                    DT_SUSPENSAO,
                    REPLACE(REPLACE(DS_MOTIVO, CHAR(13), ''), CHAR(10), '') AS DS_MOTIVO,
                    REPLACE(REPLACE(DS_PREVISAO, CHAR(13), ''), CHAR(10), '') AS DS_PREVISAO,
                    REPLACE(REPLACE(DS_JUSTIFICATIVA, CHAR(13), ''), CHAR(10), '') AS DS_JUSTIFICATIVA,
                    NM_RESPONSAVEL
            FROM dbo.fuSEGOVI_Obras_Suspensas()
            """,
    },
    {
        "table_id": "itens_medicao",
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT
                DISTINCT
                    CD_OBRA,
                    REPLACE(REPLACE(DS_TITULO_OBJETO, CHAR(13), ''), CHAR(10), '') AS DS_TITULO_OBJETO,
                    DS_ESTADO,
                    NR_MEDICAO,
                    DT_INI_MEDICAO,
                    DT_FIM_MEDICAO,
                    CD_ETAPA,
                    NM_SISTEMA,
                    NM_SUB_SISTEMA,
                    NM_PLANILHA,
                    NR_ITEM,
                    CD_CHAVE_EXTERNA,
                    REPLACE(REPLACE(DS_ITEM_SERVICO, CHAR(13), ''), CHAR(10), '') AS DS_ITEM_SERVICO,
                    TX_UNIDADE_MEDIDA,
                    VL_ITEM_SERVICO,
                    QT_MEDIDA,
                    QT_ACUMULADA,
                    VL_MEDIDO
            FROM dbo.fuSEGOVI_Itens_Medicao()
            """,
    },
    {
        "table_id": "orcamento_licitado",
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT
                DISTINCT
                    CD_OBRA,
                    REPLACE(REPLACE(DS_TITULO_OBJETO, CHAR(13), ''), CHAR(10), '') AS DS_TITULO_OBJETO,
                    NM_SISTEMA,
                    NM_SUB_SISTEMA,
                    NM_PLANILHA,
                    NR_ITEM,
                    CD_CHAVE_EXTERNA,
                    REPLACE(REPLACE(DS_ITEM_SERVICO, CHAR(13), ''), CHAR(10), '') AS DS_ITEM_SERVICO,
                    TX_UNIDADE_MEDIDA,
                    QT_CONTRATADO,
                    VL_UNITARIO,
                    VL_TOTAL
            FROM dbo.fuSEGOVI_Orcamento_Licitado()
            """,
    },
    {
        "table_id": "itens_medidos_finalizados",
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT
                DISTINCT
                    CD_OBRA,
                    NM_SISTEMA,
                    NM_SUB_SISTEMA,
                    NM_PLANILHA,
                    NR_ITEM,
                    CD_CHAVE_EXTERNA,
                    REPLACE(REPLACE(DS_ITEM_SERVICO, CHAR(13), ''), CHAR(10), '') AS DS_ITEM_SERVICO,
                    TX_UNIDADE_MEDIDA,
                    QT_CONTRATADA,
                    VL_UNITARIO_LICITACAO,
                    QT_ACUMULADA,
                    VL_ACUMULADO_MEDIDO,
                    DT_FIM_OBRA
            FROM dbo.fuSEGOVI_Itens_Medidos_Finalizados()
        """,
    },
    {
        "table_id": "localizacao_obra",
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT
                DISTINCT
                    CD_OBRA,
                    ENDERECO,
                    NM_BAIRRO,
                    NM_RA,
                    NM_AP
            FROM dbo.fuSEGOVI_Localizacoes_obra()
        """,
    },
    {
        "table_id": "programa_fonte",
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT
                DISTINCT
                    CD_OBRA,
                    CD_PRG_TRAB,
                    PROGRAMA_TRABALHO,
                    CD_FONTE_RECURSO,
                    FONTE_RECURSO,
                    CD_NATUREZA_DSP,
                    NATUREZA_DESPESA
            FROM dbo.fuSEGOVI_Programa_Fonte()
            """,
    },
    {
        "table_id": "fiscal",
        "dump_mode": "overwrite",
        "execute_query": """
            SELECT
                CD_OBRA,
                DT_INICIO_VIG,
                DT_FIM_VIG,
                MATRICULA,
                NOME,
                EMAIL
            FROM dbo.fuSEGOVI_Fiscais()
            """,
    },
    # {
    #     "table_id": "localizacao",
    #     "dump_mode": "overwrite",
    #     "execute_query": """
    #         SELECT
    #             DISTINCT
    #                 CD_OBRA,
    #                 ENDERECO,
    #                 NM_BAIRRO,
    #                 NM_RA,
    #                 NM_AP
    #         FROM dbo.()
    #         """,
    # },
    # {
    #     "table_id": "orcamento_medicao",
    #     "dump_mode": "overwrite",
    #     "execute_query": """
    #         SELECT
    #         FROM dbo.()
    #     """,
    # },
]

# General Deployment Settings
BASE_ANCHOR_DATE = "2025-07-15T01:00:00"
BASE_INTERVAL_SECONDS = 3600 * 24  # Run each table every day
RUNS_SEPARATION_MINUTES = 10  # Stagger start times by 10 minutes
TIMEZONE = "America/Sao_Paulo"

# --- Generate and Print YAML ---
schedules_config = create_dump_db_schedules(
    table_parameters_list=SISCOB_QUERIES,
    base_interval_seconds=BASE_INTERVAL_SECONDS,
    base_anchor_date_str=BASE_ANCHOR_DATE,
    runs_interval_minutes=RUNS_SEPARATION_MINUTES,
    timezone=TIMEZONE,
)

# Use sort_keys=False to maintain the intended order of keys in the output
print(schedules_config)
