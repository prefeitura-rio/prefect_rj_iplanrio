# -*- coding: utf-8 -*-
"""
Constantes especificas para pipeline PIC SMAS
"""

from enum import Enum


class PicConstants(Enum):
    """
    Constantes para o pipeline de disparo PIC SMAS
    """

    # HSM Template ID para mensagens PIC
    PIC_ID_HSM = 180

    # Nome da campanha
    PIC_CAMPAIGN_NAME = "jp-teste-pipeline-whatsapp-pic"

    # Cost Center ID
    PIC_COST_CENTER_ID = 1

    # Billing Project ID
    PIC_BILLING_PROJECT_ID = "rj-smas"

    # Query processor name - handles dynamic D+2 date calculation
    PIC_QUERY_PROCESSOR_NAME = "pic"

    # Configuracoes de dataset
    PIC_DATASET_ID = "brutos_wetalkie"
    PIC_TABLE_ID = "disparos_efetuados"
    PIC_DUMP_MODE = "append"
    PIC_CHUNK_SIZE = 1000

    # Query MOCK para testes - João Pedro Oliveira
    # ATENÇÃO: Esta é a query de TESTE, não dispara para base real!
    PIC_QUERY = r"""
        SELECT
            '5521985573582' AS telefone,
            'João Pedro Oliveira' AS nome_sobrenome,
            '12345678901' AS cpf,
            'Rua Teste, 123 - Centro (Espaço de Teste)' AS endereco,
            FORMAT_DATE('%d/%m/%Y', DATE_ADD(CURRENT_DATE(), INTERVAL 2 DAY)) AS data,
            '10:00' AS horario
    """

    # Query principal do PIC - usa estrutura híbrida com CTEs e filtro dinâmico D+2
    # COMENTADO PARA TESTES - DESCOMENTAR PARA PRODUÇÃO
    # PIC_QUERY = r"""
    """    WITH agendamentos_unicos AS (
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

            -- Endereço concatenado
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

            -- Telefone validado com fallback
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
          WHERE t1.DATA_ENTREGA_PREVISTA = '{data_evento}'
        )
        SELECT
          celular_disparo AS telefone,
          -- Nome formatado: primeiro + último nome
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
          ) AS nome,
          cpf,
          endereco_evento AS endereco,
          FORMAT_DATE('%d/%m/%Y', data_evento) AS data,
          horario_evento AS horario
        FROM joined_status_cpi
        WHERE celular_disparo IS NOT NULL
    # """
