# -*- coding: utf-8 -*-
# flake8: noqa:E501
# pylint: disable='line-too-long'
"""
Constantes específicas para pipeline CadÚnico SMAS
"""

from enum import Enum


class CadunicoConstants(Enum):
    """
    Constantes para o pipeline de disparo CadÚnico SMAS
    """

    # HSM Template ID para mensagens CadÚnico
    CADUNICO_ID_HSM = 101

    # Nome da campanha
    CADUNICO_CAMPAIGN_NAME = "smas-lembretecadunico-prod"

    # Cost Center ID
    CADUNICO_COST_CENTER_ID = 71

    # Billing Project ID
    CADUNICO_BILLING_PROJECT_ID = "rj-crm-registry"

    # Query processor name
    CADUNICO_QUERY_PROCESSOR_NAME = ""

    # Configurações de dataset
    CADUNICO_DATASET_ID = "brutos_cadunico"
    CADUNICO_TABLE_ID = "disparos"
    CADUNICO_DUMP_MODE = "append"
    CADUNICO_CHUNK_SIZE = 1000

    DAYS_AHEAD = 2

    # Query principal do CadÚnico
    CADUNICO_QUERY = r"""
        WITH
        source_ AS (
        SELECT
            `rj-crm-registry.udf.VALIDATE_AND_FORMAT_CELLPHONE`(REGEXP_REPLACE(telefone, r'[^\d]', '')) AS celular_disparo,
            primeiro_nome,
            data_hora,
            unidade_nome,
            unidade_endereco,
            unidade_bairro,
            cpf
        FROM `rj-iplanrio.brutos_data_metrica_staging.cadunico_agendamentos`
        WHERE
            DATE(data_hora) = DATE_ADD( CURRENT_DATE("America/Sao_Paulo"), INTERVAL CAST({days_ahead_placeholder} AS int64) DAY) ),
        celulares_disparados AS (
        SELECT
            `rj-crm-registry.udf.VALIDATE_AND_FORMAT_CELLPHONE`(tel.contato_telefone) AS contato_telefone
        FROM `rj-crm-registry.crm_whatsapp.telefone_disparado` tel
        WHERE
            id_hsm = cast({id_hsm_placeholder} as string)
            AND data_particao >= DATE_SUB(CURRENT_DATE(), INTERVAL 15 DAY) ),
        filtra_celulares_disparados AS (
        SELECT
            source_.*
        FROM source_
        LEFT JOIN celulares_disparados
        ON source_.celular_disparo = contato_telefone
        WHERE
            celulares_disparados.contato_telefone IS NULL ),
        filtra_celulares_sem_whats AS (
        SELECT
            DISTINCT s.*
        FROM filtra_celulares_disparados AS s
        LEFT JOIN `rj-crm-registry.intermediario_rmi_telefones.int_telefone` AS tel
        ON s.celular_disparo = tel.telefone_numero_completo
        LEFT JOIN
            UNNEST(tel.consentimento) AS c
        WHERE
            (c.indicador_quarentena = FALSE
            AND tel.telefone_qualidade != "INVALIDO")
            OR tel.telefone_numero_completo IS NULL )
        SELECT
        TO_JSON_STRING(STRUCT( celular_disparo,
            STRUCT( primeiro_nome AS NOME,
                FORMAT_DATETIME('%d/%m/%Y', DATETIME(data_hora)) AS DATA,
                FORMAT_DATETIME('%H:%M', DATETIME(data_hora)) AS HORA,
                unidade_nome AS LOCAL,
                CONCAT(unidade_endereco, ' - ', unidade_bairro) AS ENDERECO,
            cpf as CC_WT_CPF_CIDADAO ) AS vars,
            cpf AS externalId )
        ) AS destination_data
        FROM filtra_celulares_sem_whats
    """

    QUERY_MOCK = r"""
    WITH
        config AS (
          SELECT
            {days_ahead_placeholder} AS days_ahead,
            {id_hsm_placeholder} AS id_hsm
        )
        select '{"celular_disparo":"5511984677798","vars":{"NOME":"Patricia","DATA":"05/11/2025","HORA":"11:00","LOCAL":"Cras Tijuca","ENDERECO":"Rua Guapiara - Nº 43 - Tijuca","CC_WT_CPF_CIDADAO":"06347059876"},"externalId":"06347059876"}'
    """

    # CREATE_CADUNICO_MOCK_TABLES = r"""
    #   -- Criação da tabela de mock para cadunico_agendamentos
    #   CREATE or replace TABLE `rj-crm-registry-dev.teste.cadunico_agendamentos_mock` (
    #     telefone STRING,
    #     primeiro_nome STRING,
    #     data_hora DATETIME,
    #     unidade_nome STRING,
    #     unidade_endereco STRING,
    #     unidade_bairro STRING,
    #     cpf STRING
    #   );

    #   -- Inserção de dados fake na tabela de mock de cadunico_agendamentos
    #   INSERT INTO `rj-crm-registry-dev.teste.cadunico_agendamentos_mock` (
    #     telefone,
    #     primeiro_nome,
    #     data_hora,
    #     unidade_nome,
    #     unidade_endereco,
    #     unidade_bairro,
    #     cpf
    #   )
    #   VALUES
    #     ('5511984677798', 'Patricia', DATETIME '2025-11-05 11:00:00', 'Cras Tijuca', 'Rua Guapiara - Nº 43', 'Tijuca', '06347059876'),
    #     ('5521987654321', 'João', DATETIME '2025-11-05 10:00:00', 'Cras Centro', 'Rua do Lavradio, 123', 'Centro', '12345678901');

    #   -- Criação da tabela de mock para telefone_disparado
    #   CREATE or replace TABLE `rj-crm-registry-dev.teste.telefone_disparado_mock` (
    #     contato_telefone STRING,
    #     id_hsm STRING,
    #     data_particao DATE
    #   );

    #   -- Inserção de dados fake na tabela de mock de telefone_disparado
    #   INSERT INTO `rj-crm-registry-dev.teste.telefone_disparado_mock` (
    #     contato_telefone,
    #     id_hsm,
    #     data_particao
    #   )
    #   VALUES
    #     ('5511984677799', '101', CURRENT_DATE()), -- Example of a number that was already sent
    #     ('5521987654322', '101', CURRENT_DATE());

    #   -- Criação da tabela de mock para int_telefone (simplified for this context)
    #   CREATE or replace TABLE `rj-crm-registry-dev.teste.int_telefone_mock` (
    #     telefone_numero_completo STRING,
    #     telefone_qualidade STRING,
    #     consentimento ARRAY<STRUCT<indicador_quarentena BOOL>>
    #   );

    #   -- Inserção de dados fake na tabela de mock de int_telefone
    #   INSERT INTO `rj-crm-registry-dev.teste.int_telefone_mock` (
    #     telefone_numero_completo,
    #     telefone_qualidade,
    #     consentimento
    #   )
    #   VALUES
    #     ('5511984677798', 'VALIDO', [STRUCT(FALSE)]),
    #     ('5521987654321', 'VALIDO', [STRUCT(FALSE)]),
    #     ('5511984677799', 'VALIDO', [STRUCT(FALSE)]),
    #     ('5521987654322', 'VALIDO', [STRUCT(FALSE)]),
    #     ('5599999999999', 'INVALIDO', [STRUCT(FALSE)]), -- Example of an invalid number
    #     ('5588888888888', 'VALIDO', [STRUCT(TRUE)]);    -- Example of a quarantined number
    # """


    # CADUNICO_QUERY_MOCK = r"""
    #     WITH
    #     config AS (
    #       SELECT
    #         {days_ahead_placeholder} AS days_ahead,
    #         {id_hsm_placeholder} AS id_hsm
    #     ),
    #     source_ AS (
    #     SELECT
    #         `rj-crm-registry.udf.VALIDATE_AND_FORMAT_CELLPHONE`(REGEXP_REPLACE(telefone, r'[^\d]', '')) AS celular_disparo,
    #         primeiro_nome,
    #         data_hora,
    #         unidade_nome,
    #         unidade_endereco,
    #         unidade_bairro,
    #         cpf
    #     FROM `rj-crm-registry-dev.teste.cadunico_agendamentos_mock`
    #     WHERE
    #         DATE(data_hora) = DATE_ADD( CURRENT_DATE("America/Sao_Paulo"), INTERVAL CAST((SELECT days_ahead_placeholder FROM config) AS int64) DAY) ),
    #     celulares_disparados AS (
    #     SELECT
    #         `rj-crm-registry.udf.VALIDATE_AND_FORMAT_CELLPHONE`(tel.contato_telefone) AS contato_telefone
    #     FROM `rj-crm-registry-dev.teste.telefone_disparado_mock` tel
    #     WHERE
    #         id_hsm = cast((SELECT id_hsm_placeholder FROM config) as string)
    #         AND data_particao >= DATE_SUB(CURRENT_DATE(), INTERVAL 15 DAY) ),
    #     filtra_celulares_disparados AS (
    #     SELECT
    #         source_.*
    #     FROM source_
    #     LEFT JOIN celulares_disparados
    #     ON source_.celular_disparo = contato_telefone
    #     WHERE
    #         celulares_disparados.contato_telefone IS NULL ),
    #     filtra_celulares_sem_whats AS (
    #     SELECT
    #         DISTINCT s.*
    #     FROM filtra_celulares_disparados AS s
    #     LEFT JOIN `rj-crm-registry-dev.teste.int_telefone_mock` AS tel
    #     ON s.celular_disparo = tel.telefone_numero_completo
    #     LEFT JOIN
    #         UNNEST(tel.consentimento) AS c
    #     WHERE
    #         (c.indicador_quarentena = FALSE
    #         AND tel.telefone_qualidade != "INVALIDO")
    #         OR tel.telefone_numero_completo IS NULL )
    #     SELECT
    #     TO_JSON_STRING(STRUCT( celular_disparo,
    #         STRUCT( primeiro_nome AS NOME,
    #             FORMAT_DATETIME('%d/%m/%Y', DATETIME(data_hora)) AS DATA,
    #             FORMAT_DATETIME('%H:%M', DATETIME(data_hora)) AS HORA,
    #             unidade_nome AS LOCAL,
    #             CONCAT(unidade_endereco, ' - ', unidade_bairro) AS ENDERECO,
    #         cpf as CC_WT_CPF_CIDADAO ) AS vars,
    #         cpf AS externalId )
    #     ) AS destination_data
    #     FROM filtra_celulares_sem_whats
    # """
