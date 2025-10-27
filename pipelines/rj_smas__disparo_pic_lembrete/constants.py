# -*- coding: utf-8 -*-
"""Constantes específicas para pipeline PIC lembrete SMAS."""

from enum import Enum


class PicLembreteConstants(Enum):
    """Constantes do pipeline de disparo PIC lembrete SMAS."""

    # HSM Template ID para mensagens PIC
    PIC_LEMBRETE_ID_HSM = 185

    # Nome da campanha
    PIC_LEMBRETE_CAMPAIGN_NAME = "smas-cartaopic-lembrete"

    # Cost Center ID
    PIC_LEMBRETE_COST_CENTER_ID = 38

    # Billing Project ID
    PIC_LEMBRETE_BILLING_PROJECT_ID = "rj-smas"

    # Query processor name (mantido vazio para desativar processadores dinâmicos)
    PIC_LEMBRETE_QUERY_PROCESSOR_NAME = ""

    # Configurações de dataset / tabela para registro dos disparos
    PIC_LEMBRETE_DATASET_ID = "brutos_wetalkie"
    PIC_LEMBRETE_TABLE_ID = "disparos_efetuados"
    PIC_LEMBRETE_DUMP_MODE = "append"
    PIC_LEMBRETE_CHUNK_SIZE = 1000

    # Modo de teste - ativar por padrão para segurança
    PIC_LEMBRETE_TEST_MODE = True

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
          WHERE SAFE.PARSE_DATE('%Y-%m-%d', TRIM(CAST(t1.DATA_ENTREGA_PREVISTA AS STRING))) = config.target_date
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

    DISPATCH_APPROVED_COL = "APROVACAO_DISPARO_LEMBRETE"  # column that says if the dispatch was approved
    DISPATCH_DATE_COL = "DATA_DISPARO_LEMBRETE"  # column that contains the date to be made the dispatch
    EVENT_DATE_COL = "DATA_ENTREGA"  # columns with the event date (date to get the PIC)

    PIC_QUERY_DISPATCH_APPROVED = r"""
      select * from `rj-smas-dev.pic.raw_cartao_primeira_infancia_carioca_bairros_entrega`
    """

    PIC_QUERY_MOCK_DISPATCH_APPROVED = r"""
      select * from `rj-crm-registry-dev.brutos_wetalkie_staging.aprovacao_evento`
    """

    # Query mock para testes rápidos (não dispara para base real)
    PIC_QUERY_MOCK = r"""
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
          FROM `rj-crm-registry-dev.teste.cartao_primeira_infancia_carioca_status_teste` AS t1
          LEFT JOIN agendamentos_unicos AS a
            ON LPAD(CAST(t1.NUM_CPF_RESPONSAVEL AS STRING), 11, '0') = a.cpf AND a.rn = 1
          LEFT JOIN `rj-crm-registry.rmi_dados_mestres.pessoa_fisica` AS rmi
            ON LPAD(CAST(t1.NUM_CPF_RESPONSAVEL AS STRING), 11, '0') = rmi.cpf
          LEFT JOIN telefones_alternativos_rmi AS tel_alt
            ON LPAD(CAST(t1.NUM_CPF_RESPONSAVEL AS STRING), 11, '0') = tel_alt.cpf AND tel_alt.rn = 1
          CROSS JOIN config
          WHERE  SAFE.PARSE_DATE('%Y-%m-%d', TRIM(CAST(t1.DATA_ENTREGA_PREVISTA AS STRING))) = config.target_date
        ),
        filtra_quem_nao_recebeu_hsm as (
          select joined_status_cpi.*
          from joined_status_cpi
          left join `rj-crm-registry.brutos_wetalkie_staging.fluxo_atendimento_*` fl
            on fl.flattarget = joined_status_cpi.celular_disparo and fl.templateId = 280
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
                data_formatada AS DIA,
                horario_evento AS HORARIO
              ) AS vars,
              cpf AS externalId
            )
          ) AS destination_data
        FROM formatted
    """

    CREATE_MOCK_TABLES = r"""
      -- Criação da tabela de aprovação dos disparos
      CREATE or replace TABLE `rj-crm-registry-dev.brutos_wetalkie_staging.aprovacao_evento` (
        APROVACAO_DISPARO_AVISO string,             -- Indicates if the dispatch was approved
        APROVACAO_DISPARO_LEMBRETE string,
        DATA_DISPARO_AVISO STRING,         -- Date when the dispatch should be made
        DATA_DISPARO_LEMBRETE STRING,
        DATA_ENTREGA STRING                    -- Date of the event (to get the PIC)

      );

      -- Inserção de dados fake na tabela de aprovação dos disparos
      INSERT INTO `rj-crm-registry-dev.brutos_wetalkie_staging.aprovacao_evento` (
        APROVACAO_DISPARO_AVISO,
        APROVACAO_DISPARO_LEMBRETE,
        DATA_DISPARO_AVISO,
        DATA_DISPARO_LEMBRETE,
        DATA_ENTREGA
      )
      VALUES
        ("aprovado", "aprovado", cast(date_sub(current_date(), interval 5 day) as string), cast(current_date() as string), cast(date_add(current_date(), interval 500 day) as string)),
        -- ("nao aprovado",  "nao aprovado", cast(current_date() as string), cast(date_add(current_date(), interval 4 day) as string), cast(date_add(current_date(), interval 5 day) as string)),
        ("nao aprovado", "nao aprovado", '2025-09-25', '2025-09-27', '2025-09-28'),
        ("aprovado", "aprovado", '2025-09-26','2025-09-29', '2025-09-30'),
        ("nao aprovado","nao aprovado", '2025-09-27', '2025-09-30', '2025-10-01'),
        ("aprovado", "aprovado", '2025-10-01', '2025-10-04', '2025-10-05');


      -- Criação da tabela com dados das pessoas por evento
      CREATE or replace TABLE `rj-crm-registry-dev.teste.cartao_primeira_infancia_carioca_status_teste` (
        NUM_CPF_RESPONSAVEL STRING,
        NOME_RESPONSAVEL STRING,
        NUM_TEL_CONTATO_1_FAM STRING,
        NUM_TEL_CONTATO_2_FAM STRING,
        LOCAL_ENTREGA_PREVISTO STRING,
        ENDERECO_ENTREGA_PREVISTO STRING,
        DATA_ENTREGA_PREVISTA DATE,
        HORA_ENTREGA_PREVISTA STRING,
        CRAS STRING,
        ENDERECO_CRAS STRING,
        DATA_RETIRADA_CRAS DATE,
        HORA_RETIRADA_CRAS STRING,
        ENVELOPE STRING,
        NUM_TEL_CONTATO_1_DECLARADO STRING,
        NOME_SOCIAL_RESPONSAVEL STRING,
        DATA_ENTREGA DATE,
        TIPO_ENTREGA STRING,
        LOCAL_ENTREGA STRING,
        RESPONSAVEL_PELA_RETIRADA STRING,
        STATUS STRING,
        FLAG_ENTREGA BOOLEAN
      );

      -- Inserção de dados falsos para a tabela com dados das pessoas por evento
      INSERT INTO `rj-crm-registry-dev.teste.cartao_primeira_infancia_carioca_status_teste` (
        NUM_CPF_RESPONSAVEL,
        NOME_RESPONSAVEL,
        NUM_TEL_CONTATO_1_FAM,
        NUM_TEL_CONTATO_2_FAM,
        LOCAL_ENTREGA_PREVISTO,
        ENDERECO_ENTREGA_PREVISTO,
        DATA_ENTREGA_PREVISTA,
        HORA_ENTREGA_PREVISTA,
        CRAS,
        ENDERECO_CRAS,
        DATA_RETIRADA_CRAS,
        HORA_RETIRADA_CRAS,
        ENVELOPE,
        NUM_TEL_CONTATO_1_DECLARADO,
        NOME_SOCIAL_RESPONSAVEL,
        DATA_ENTREGA,
        TIPO_ENTREGA,
        LOCAL_ENTREGA,
        RESPONSAVEL_PELA_RETIRADA,
        STATUS,
        FLAG_ENTREGA
      )
      VALUES
        ('111111111111', 'patricia catandi', '5511984677798', null, 'Cras Vila Isabel', 'Rua Torres Homem, 120 - Vila Isabel', date_add(current_date(), interval 500 day), '10:00', 'CRAS Vila Isabel', 'Rua Torres Homem, 120 - Vila Isabel', date_add(current_date(), interval 400 day), '10:15', 'E001', 'xxx 99999-1111', 'Maria S.', date_add(current_date(), interval 400 day), 'retirada no CRAS', 'CRAS Vila Isabel', 'MARIA DA SILVA', 'aguardando evento', TRUE),

        ('xx765432100', 'JOÃO PEREIRA', 'xxx 97777-3333', 'xxx 96666-4444', 'Cras Bangu', 'Rua Fonseca, 300 - Bangu', DATE '2024-10-28', '14:00', 'CRAS Bangu', 'Rua Fonseca, 300 - Bangu', NULL, NULL, 'E002', 'xxx 97777-3333', NULL, NULL, 'entrega domiciliar', 'Endereço familiar', NULL, 'aguardando evento', FALSE),

        ('xx432198700', 'ANA OLIVEIRA', 'xxx 95555-5555', NULL, 'Cras Madureira', 'Rua Carvalho, 85 - Madureira', DATE '2024-10-27', '09:30', 'CRAS Madureira', 'Rua Carvalho, 85 - Madureira', DATE '2024-10-27', '09:45', 'E003', 'xxx 95555-5555', 'Ana O.', DATE '2024-10-27', 'retirada no CRAS', 'CRAS Madureira', 'ANA OLIVEIRA', 'retirado', TRUE),

        ('xx296374100', 'CARLOS SOUZA', 'xxx 94444-6666', 'xxx 93333-7777', 'Cras Campo Grande', 'Av. Cesário de Melo, 3200 - Campo Grande', DATE '2024-10-30', '13:00', 'CRAS Campo Grande', 'Av. Cesário de Melo, 3200 - Campo Grande', NULL, NULL, 'E004', 'xxx 94444-6666', NULL, NULL, 'entrega domiciliar', 'Endereço familiar', NULL, 'não retirado', FALSE),

        ('xx223344556', 'PAULA MENDES', 'xxx 92222-8888', NULL, 'Cras Tijuca', 'Rua Conde de Bonfim, 900 - Tijuca', DATE '2024-10-29', '11:00', 'CRAS Tijuca', 'Rua Conde de Bonfim, 900 - Tijuca', DATE '2024-10-29', '11:10', 'E005', 'xxx 92222-8888', 'Paula M.', DATE '2024-10-29', 'retirada no CRAS', 'CRAS Tijuca', 'PAULA MENDES', 'retirado', TRUE);

    """
