# -*- coding: utf-8 -*-
"""Constantes específicas para pipeline PIC lembrete SMAS."""

from enum import Enum


class PicLembreteConstants(Enum):
    """Constantes do pipeline de disparo PIC lembrete SMAS."""

    # HSM Template ID para mensagens PIC
    PIC_LEMBRETE_ID_HSM = 180

    # Nome da campanha
    PIC_LEMBRETE_CAMPAIGN_NAME = "smas-lembrete-pic"

    # Cost Center ID
    PIC_LEMBRETE_COST_CENTER_ID = 1

    # Billing Project ID
    PIC_LEMBRETE_BILLING_PROJECT_ID = "rj-smas"

    # Query processor name - aplica D+2 no placeholder {data_evento}
    PIC_LEMBRETE_QUERY_PROCESSOR_NAME = "pic_lembrete"

    # Configurações de dataset / tabela para registro dos disparos
    PIC_LEMBRETE_DATASET_ID = "brutos_wetalkie"
    PIC_LEMBRETE_TABLE_ID = "disparos_efetuados"
    PIC_LEMBRETE_DUMP_MODE = "append"
    PIC_LEMBRETE_CHUNK_SIZE = 1000

    # Query mock para testes rápidos (não dispara para base real)
    PIC_LEMBRETE_QUERY_MOCK = r"""
        SELECT
          TO_JSON_STRING(
            STRUCT(
              '5521980000000' AS celular_disparo,
              STRUCT(
                'Cidadã Teste' AS NOME_SOBRENOME,
                '12345678901' AS CPF,
                'Endereço Exemplo, 123 - Centro' AS ENDERECO,
                FORMAT_DATE('%d/%m/%Y', DATE('{data_evento}')) AS DIA,
                '10:00' AS HORARIO
              ) AS vars,
              '12345678901' AS externalId
            )
          ) AS destination_data
    """

    # Query principal do PIC lembrete com saída em JSON (destination_data)
    PIC_LEMBRETE_QUERY = r"""
        WITH agendamentos_unicos AS (
          SELECT
            LPAD(cpf, 11, '0') AS cpf,
            telefone,
            ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY data_hora DESC) AS rn
          FROM `rj-smas.brutos_data_metrica_staging.agendamentos_cadunico`
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
          WHERE t1.DATA_ENTREGA_PREVISTA = DATE('{data_evento}')
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
            FORMAT_DATE('%d/%m/%Y', data_evento) AS data_formatada,
            horario_evento
          FROM joined_status_cpi
          WHERE celular_disparo IS NOT NULL
        )
        SELECT
          TO_JSON_STRING(
            STRUCT(
              celular_disparo AS celular_disparo,
              STRUCT(
                nome_sobrenome AS NOME_SOBRENOME,
                cpf AS CPF,
                endereco_evento AS ENDERECO,
                data_formatada AS DATA,
                horario_evento AS HORARIO
              ) AS vars,
              cpf AS externalId
            )
          ) AS destination_data
        FROM formatted
    """
