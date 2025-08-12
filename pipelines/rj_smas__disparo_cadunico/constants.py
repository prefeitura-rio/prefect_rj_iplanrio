# -*- coding: utf-8 -*-
"""
Constantes específicas para pipeline CadÚnico SMAS
Migrado de pipelines_rj_crm_registry/pipelines/smas/CADUNICO/constants.py
"""

from enum import Enum


class CadunicoConstants(Enum):
    """
    Constantes para o pipeline de disparo CadÚnico SMAS
    """
    
    # HSM Template ID para mensagens CadÚnico
    CADUNICO_HSM_ID = 101
    
    # Nome da campanha
    CADUNICO_CAMPAIGN_NAME = "smas-lembretecadunico-prod"
    
    # Cost Center ID  
    CADUNICO_COST_CENTER_ID = 1
    
    # Query processor name
    CADUNICO_QUERY_PROCESSOR = "cadunico"
    
    # Query principal do CadÚnico
    CADUNICO_QUERY = """
    SELECT
        TO_JSON_STRING(STRUCT(
            REGEXP_REPLACE(telefone, r'[^\\d]', '') as celular_disparo,
            STRUCT(
                primeiro_nome as NOME,
                FORMAT_DATETIME('%d/%m/%Y', DATETIME(data_hora)) as DATA,
                FORMAT_DATETIME('%H:%M', DATETIME(data_hora)) as HORA,
                unidade_nome as LOCAL,
                CONCAT(unidade_endereco, ' - ', unidade_bairro) as ENDERECO
            ) as vars,
            cpf as external_id
        )) as destination_data
    FROM `rj-smas.brutos_data_metrica_staging.agendamentos_cadunico`
    WHERE
        DATE(data_hora) = DATE_ADD(
        CURRENT_DATE("America/Sao_Paulo"),
        INTERVAL {days_ahead} DAY)
        AND telefone NOT IN (
            SELECT contato_telefone
            FROM `rj-crm-registry.crm_whatsapp.telefone_sem_whatsapp`
            WHERE data_atualizacao > DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
        )
        AND telefone IS NOT NULL
        AND LENGTH(REGEXP_REPLACE(telefone, r'[^\\d]', '')) >= 10
        AND telefone NOT IN (
            SELECT contato_telefone
            FROM `rj-crm-registry-dev.patricia__crm_whatsapp.telefone_disparado`
            WHERE id_hsm = {hsm_id}
                AND data_particao >= DATE_SUB(CURRENT_DATE(), INTERVAL 15 DAY)
        )
    ORDER BY data_hora
    LIMIT 0
    """
    
    # Configurações de projeto e dataset
    CADUNICO_BILLING_PROJECT_ID = "rj-crm-registry"
    CADUNICO_DATASET_ID = "disparos"
    CADUNICO_TABLE_ID = "disparos_efetuados"