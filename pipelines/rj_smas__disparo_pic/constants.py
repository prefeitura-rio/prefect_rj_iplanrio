# -*- coding: utf-8 -*-
"""Constantes específicas para pipeline PIC lembrete SMAS."""

from enum import Enum


class PicLembreteConstants(Enum):
    """Constantes do pipeline de disparo PIC lembrete SMAS."""

    # HSM Template ID para mensagens PIC
    PIC_ID_HSM = 184

    # Nome da campanha
    PIC_CAMPAIGN_NAME = "smas-cartaopic-disparoevento"

    # Cost Center ID
    PIC_COST_CENTER_ID = 38

    # Billing Project ID
    PIC_BILLING_PROJECT_ID = "rj-smas"

    # Query processor name (mantido vazio para desativar processadores dinâmicos)
    PIC_QUERY_PROCESSOR_NAME = ""

    # Configurações de dataset / tabela para registro dos disparos
    PIC_DATASET_ID = "brutos_wetalkie"
    PIC_TABLE_ID = "disparos_efetuados"
    PIC_DUMP_MODE = "append"
    PIC_CHUNK_SIZE = 1000

    # Modo de teste - ativar por padrão para segurança
    PIC_TEST_MODE = True

    # Query mock para testes rápidos (não dispara para base real)
    PIC_QUERY_MOCK = r"""
        WITH config AS (
          select date('{event_date_placeholder}') AS target_date
        ),
        test_data AS (
          SELECT 1 AS id, '5511984677798' AS celular_disparo, 'Joao Santos' AS nome, '11111111111' AS cpf
          # UNION ALL SELECT 2, '5521992132305', 'Bruno Mesquita', '22222222222'
           UNION ALL SELECT 3, '5511984677798', 'Patricia Catandi', '33333333333'
          #  UNION ALL SELECT 4, '559284212629', 'Francisco Leon', '44444444444'
          #  UNION ALL SELECT 5, '5521985573582', 'Joao Santos', '555555555555'
        ),
        filtra_quem_nao_recebeu_hsm as (
          select test_data.*
          from test_data
          left join `rj-crm-registry.brutos_wetalkie_staging.fluxo_atendimento_*` fl
            on fl.flattarget = test_data.celular_disparo and fl.templateId = {id_hsm_placeholder}
          where fl.flattarget is null
        )
        SELECT
          TO_JSON_STRING(
            STRUCT(
              celular_disparo,
              STRUCT(
                nome AS NOME_SOBRENOME,
                cpf AS CC_WT_CPF_CIDADAO,
                'Endereço Teste, 123 - Centro' AS ENDERECO,
                FORMAT_DATE('%d/%m/%Y', config.target_date) AS DATA,
                '10:00' AS HORARIO
              ) AS vars,
              cpf AS externalId
            )
          ) AS destination_data
        FROM filtra_quem_nao_recebeu_hsm
        CROSS JOIN config
    """

    # Query principal do PIC lembrete com saída em JSON (destination_data)
    PIC_QUERY = r"""
        WITH config AS (
        select date('{event_date_placeholder}') AS target_date
        ),
        agendamentos_unicos AS (
          SELECT
            LPAD(cpf, 11, '0') AS cpf,
            telefone,
            ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY data_hora DESC) AS rn
          FROM `rj-crm-registry.brutos_data_metrica_staging.agendamentos_cadunico`
          WHERE cpf IS NOT NULL
        ),
        telefones_alternativos_rmi AS (
          SELECT
            cpf,
            CONCAT(tel_alt.ddi, tel_alt.ddd, tel_alt.valor) AS telefone_alternativo,
            ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY tel_alt.origem) AS rn
          FROM `rj-crm-registry.rmi_dados_mestres.pessoa_fisica` AS rmi,
          UNNEST(rmi.telefone.alternativo) AS tel_alt
          WHERE tel_alt.valor IS NOT NULL
        ),
        joined_status_cpi AS (
          SELECT
            TRIM(t1.NOME_RESPONSAVEL) AS nome_completo,
            LPAD(CAST(t1.NUM_CPF_RESPONSAVEL AS STRING), 11, '0') AS cpf,
            SAFE.PARSE_DATE('%Y-%m-%d', TRIM(CAST(t1.DATA_ENTREGA_PREVISTA AS STRING))) AS data_evento_date,
            TRIM(
              CONCAT(
                t1.LOCAL_ENTREGA_PREVISTO,
                IF(
                  t1.ENDERECO_ENTREGA_PREVISTO IS NOT NULL
                  AND t1.ENDERECO_ENTREGA_PREVISTO != '',
                  CONCAT(' (', t1.ENDERECO_ENTREGA_PREVISTO, ')'),
                  ''
                )
              )
            ) AS endereco_evento,
            t1.DATA_ENTREGA_PREVISTA AS data_evento,
            t1.HORA_ENTREGA_PREVISTA AS horario_evento,
            COALESCE(
              `rj-crm-registry.udf.VALIDATE_AND_FORMAT_CELLPHONE`(t1.NUM_TEL_CONTATO_1_FAM),
              `rj-crm-registry.udf.VALIDATE_AND_FORMAT_CELLPHONE`(t1.NUM_TEL_CONTATO_2_FAM),
              `rj-crm-registry.udf.VALIDATE_AND_FORMAT_CELLPHONE`(a.telefone),
              `rj-crm-registry.udf.VALIDATE_AND_FORMAT_CELLPHONE`(CONCAT("55", rmi.telefone.principal.ddd, rmi.telefone.principal.valor)),
              `rj-crm-registry.udf.VALIDATE_AND_FORMAT_CELLPHONE`(CONCAT("55", tel_alt.telefone_alternativo))
            ) AS celular_disparo
          FROM `rj-smas-dev.pic.cartao_primeira_infancia_carioca_status` AS t1
          LEFT JOIN agendamentos_unicos AS a
            ON LPAD(CAST(t1.NUM_CPF_RESPONSAVEL AS STRING), 11, '0') = a.cpf AND a.rn = 1
          LEFT JOIN `rj-crm-registry.rmi_dados_mestres.pessoa_fisica` AS rmi
            ON LPAD(CAST(t1.NUM_CPF_RESPONSAVEL AS STRING), 11, '0') = rmi.cpf
          LEFT JOIN telefones_alternativos_rmi AS tel_alt
            ON LPAD(CAST(t1.NUM_CPF_RESPONSAVEL AS STRING), 11, '0') = tel_alt.cpf AND tel_alt.rn = 1
          CROSS JOIN config
          WHERE  SAFE.PARSE_DATE('%Y-%m-%d', TRIM(CAST(t1.DATA_ENTREGA_PREVISTA AS STRING))) = config.target_date
            -- t1.DATA_ENTREGA_PREVISTA = '2025-10-29'
        ),
        filtra_quem_nao_recebeu_hsm as (
          select joined_status_cpi.*
          from joined_status_cpi
          left join `rj-crm-registry.brutos_wetalkie_staging.fluxo_atendimento_*` fl
            on fl.flattarget = joined_status_cpi.celular_disparo and fl.templateId = {id_hsm_placeholder}
          where fl.flattarget is null
        ),
        formatted AS (
          SELECT
            celular_disparo,
            INITCAP(
              IF(
                ARRAY_LENGTH(SPLIT(nome_completo, ' ')) > 1,
                CONCAT(
                  SPLIT(nome_completo, ' ')[SAFE_OFFSET(0)],
                  ' ',
                  SPLIT(nome_completo, ' ')[SAFE_OFFSET(ARRAY_LENGTH(SPLIT(nome_completo, ' ')) - 1)]
                ),
                nome_completo
              )
            ) AS nome_sobrenome,
            cpf,
            endereco_evento,
            FORMAT_DATE('%d/%m/%Y', data_evento_date) AS data_formatada,
            horario_evento
          FROM filtra_quem_nao_recebeu_hsm
          WHERE celular_disparo IS NOT NULL
            AND data_evento_date IS NOT NULL
        )
        SELECT
          TO_JSON_STRING(
            STRUCT(
              celular_disparo AS celular_disparo,
              STRUCT(
                nome_sobrenome AS NOME_SOBRENOME,
                cpf AS CC_WT_CPF_CIDADAO,
                endereco_evento AS ENDERECO,
                data_formatada AS DATA,
                horario_evento AS HORARIO
              ) AS vars,
              cpf AS externalId
            )
          ) AS destination_data
        FROM formatted
    """

    DISPATCH_APPROVED_COL = "aprovacao_disparo"  # column that says if the dispatch was approved
    DISPATCH_DATE_COL = "data_primeiro_disparo"  # column that contains the date to be made the dispatch
    EVENT_DATE_COL = "data_evento"  # columns with the event date (date to get the PIC)
    PIC_QUERY_MOCK_DISPATCH_APPROVED = r"""
      select * from `rj-crm-registry-dev.brutos_wetalkie_staging.aprovacao_evento`
    """

    PIC_QUERY_DISPATCH_APPROVED = r"""
      select * from `rj-crm-registry-dev.brutos_wetalkie_staging.aprovacao_evento`
    """
