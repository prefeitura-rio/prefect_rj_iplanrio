# -*- coding: utf-8 -*-
"""
Constantes para pipeline SISREG - Disparo de lembretes de agendamentos
"""

from enum import Enum


class SisregConstants(Enum):
    ######################################
    # SISREG Specific Constants
    ######################################

    # Dataset and table for storing dispatches
    DATASET_ID = "disparos"
    TABLE_ID = "disparos_efetuados"

    # BigQuery configuration
    BILLING_PROJECT_ID = "rj-crm-registry"
    DUMP_MODE = "append"

    # Wetalkie API configuration
    HSM_ID = 18  # HSM Template ID for SISREG messages
    CAMPAIGN_NAME = "SISREG_AGENDAMENTOS"
    COST_CENTER_ID = "2"  # TODO: Define correct cost center
    CHUNK_SIZE = 1000

    # Infisical configuration for Wetalkie credentials
    INFISICAL_SECRET_PATH = "/wetalkie"

    # BigQuery query to get SISREG appointments for next day
    QUERY = """
    WITH sisreg AS (
        SELECT
            paciente.cpf AS cpf,
            paciente.nome as NOME,
            nome_procedimento_especifico as TIPO_AGENDAMENTO,
            FORMAT_DATE('%d/%m/%Y', DATE(datahora_marcacao)) as DATA,
            TIME_TRUNC(TIME(datahora_marcacao), minute) as HORA,
            unidade.nome as UNIDADE,
            CONCAT(
            unidade.logradouro, ', ',
            unidade.numero, ' - ',
            unidade.bairro, ', ',
            unidade.municipio
        ) as ENDERECO,
        CASE
            WHEN LENGTH(unidade.telefone) = 10 THEN
                CONCAT(
                    '(', SUBSTR(unidade.telefone, 1, 2), ') ',
                    SUBSTR(unidade.telefone, 3, 4), '-',
                    SUBSTR(unidade.telefone, 7, 4)
                )
            WHEN LENGTH(unidade.telefone) = 11 THEN
                CONCAT(
                    '(', SUBSTR(unidade.telefone, 1, 2), ') ',
                    SUBSTR(unidade.telefone, 3, 5), '-',
                    SUBSTR(unidade.telefone, 8, 4)
                )
            ELSE unidade.telefone
        END as NUMERO

        FROM `rj-sms.projeto_whatsapp.agendamentos_sisreg`
        WHERE
                -- Agendamentos para o dia seguinte
                DATE(datahora_marcacao) = DATE_ADD(
                    CURRENT_DATE("America/Sao_Paulo"), INTERVAL 1 DAY)
            ORDER BY datahora_marcacao
        )

        SELECT
        TO_JSON_STRING(STRUCT(
                    CONCAT(
                        IFNULL(pf.telefone.principal.ddi, "55"),
                        IFNULL(pf.telefone.principal.ddd, "21"),
                        pf.telefone.principal.valor) AS celular_disparo,
                    STRUCT(
                        sisreg.NOME,
                        sisreg.TIPO_AGENDAMENTO,
                        sisreg.DATA,
                        sisreg.HORA,
                        sisreg.UNIDADE,
                        sisreg.ENDERECO,
                        sisreg.NUMERO
                    ) as vars
                )) as destination_data
        FROM sisreg
        INNER JOIN `rj-crm-registry.crm_dados_mestres.pessoa_fisica` as pf USING(cpf)
        WHERE pf.telefone.indicador = TRUE AND pf.obito.indicador = FALSE
        AND pf.telefone.principal.valor IS NOT NULL
    """
