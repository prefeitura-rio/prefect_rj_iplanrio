# -*- coding: utf-8 -*-
# flake8: noqa:E501
# pylint: disable='line-too-long'

"""
Funções centralizadas de validação para pipeline de template
Implementa validação robusta com logs detalhados e métricas de qualidade
"""

import os
from typing import Dict, List, Optional, Tuple

import pandas as pd
from iplanrio.pipelines_utils.logging import log  # pylint: disable=E0611, E0401
from pydantic import ValidationError

from pipelines.rj_crm__disparo_template.utils.discord import send_discord_notification  # pylint: disable=E0611, E0401
# pylint: disable=E0611, E0401
from pipelines.rj_crm__disparo_template.utils.schemas import (
    DestinationInput,
    DispatchPayload,
    SfDispatchRow,
    ValidationStats,
)


def validate_destinations(destinations: List[Dict]) -> Tuple[List[DestinationInput], ValidationStats]:
    """
    Valida lista de destinatários usando schemas Pydantic

    Args:
        destinations: Lista de dicionários com dados de destinatários

    Returns:
        Tupla contendo:
        - Lista de destinatários validados (DestinationInput)
        - Estatísticas de validação (ValidationStats)

    Raises:
        ValueError: Se nenhum destinatário for válido
    """
    if not destinations:
        log("Lista de destinatários vazia fornecida para validação")
        return [], ValidationStats(
            total_input=0, valid_records=0, invalid_records=0, validation_errors=["Lista de destinatários vazia"]
        )

    valid_destinations = []
    validation_errors = []
    total_input = len(destinations)

    log(f"Iniciando validação de {total_input} destinatários")

    for i, destination in enumerate(destinations):
        try:
            # Validação usando Pydantic schema
            validated_destination = DestinationInput(**destination)
            valid_destinations.append(validated_destination)

        except ValidationError as e:
            # Log detalhado do erro de validação
            error_details = []
            for error in e.errors():
                field = error.get("loc", ["unknown"])[0]
                message = error.get("msg", "Erro desconhecido")
                error_details.append(f"{field}: {message}")

            error_msg = f"Registro {i + 1}: {'; '.join(error_details)}"
            validation_errors.append(error_msg)

            # Log do destinatário inválido (sem dados sensíveis completos)
            to_partial = destination.get("to", "N/A")
            if isinstance(to_partial, str) and len(to_partial) > 8:
                to_partial = to_partial[:8] + "****"

            external_id = destination.get("externalId", "N/A")
            log(f"Destinatário inválido - to: {to_partial}, externalId: {external_id}, erros: {error_msg}")

        except Exception as e:
            # Erro inesperado durante validação
            error_msg = f"Registro {i + 1}: Erro inesperado - {e!s}"
            validation_errors.append(error_msg)
            log(f"Erro inesperado ao validar destinatário {i + 1}: {e!s}")

    # Compilar estatísticas
    valid_count = len(valid_destinations)
    invalid_count = total_input - valid_count

    stats = ValidationStats(
        total_input=total_input,
        valid_records=valid_count,
        invalid_records=invalid_count,
        validation_errors=validation_errors,
    )

    # Logs de resumo
    log(f"Validação concluída: {valid_count}/{total_input} destinatários válidos ({stats.success_rate:.1f}%)")

    if invalid_count > 0:
        log(f"ATENÇÃO: {invalid_count} destinatários rejeitados por problemas de validação")

        # Log detalhado dos primeiros erros (máximo 5 para não poluir)
        for error in validation_errors[:5]:
            log(f"Erro de validação: {error}")

        if len(validation_errors) > 5:
            log(f"... e mais {len(validation_errors) - 5} erros de validação")

    # Verificar se temos destinatários suficientes
    if valid_count == 0:
        raise ValueError("Nenhum destinatário válido encontrado após validação")

    # Warning se taxa de sucesso for baixa
    if stats.success_rate < 90.0:
        log(f"WARNING: Taxa de validação baixa ({stats.success_rate:.1f}%). Verificar qualidade dos dados.")

    return valid_destinations, stats


def validate_dispatch_payload(
    campaign_name: str, cost_center_id: int, destinations: List[DestinationInput]
) -> DispatchPayload:
    """
    Valida payload completo para dispatch na WeTalkie API

    Args:
        campaign_name: Nome da campanha
        cost_center_id: ID do centro de custo
        destinations: Lista de destinatários já validados

    Returns:
        DispatchPayload validado

    Raises:
        ValidationError: Se algum campo for inválido
        ValueError: Se lista de destinatários estiver vazia
    """
    try:
        log(
            f"Validando payload de dispatch: campaign='{campaign_name}', cost_center={cost_center_id}, destinations={len(destinations)}"
        )

        payload = DispatchPayload(campaignName=campaign_name, costCenterId=cost_center_id, destinations=destinations)

        log(f"Payload de dispatch validado com sucesso para {len(destinations)} destinatários")
        return payload

    except ValidationError as e:
        error_details = []
        for error in e.errors():
            field = error.get("loc", ["unknown"])[0]
            message = error.get("msg", "Erro desconhecido")
            error_details.append(f"{field}: {message}")

        error_msg = f"Payload inválido: {'; '.join(error_details)}"
        log(f"ERRO: {error_msg}")
        raise


def log_validation_summary(stats: ValidationStats, context: str = ""):
    """
    Gera log consolidado das estatísticas de validação

    Args:
        stats: Estatísticas de validação
        context: Contexto adicional para o log (ex: nome da função)
    """
    context_prefix = f"[{context}] " if context else ""

    log(f"{context_prefix}=== RESUMO DE VALIDAÇÃO ===")
    log(f"{context_prefix}Total de registros: {stats.total_input}")
    log(f"{context_prefix}Registros válidos: {stats.valid_records}")
    log(f"{context_prefix}Registros inválidos: {stats.invalid_records}")
    log(f"{context_prefix}Taxa de sucesso: {stats.success_rate:.1f}%")

    if stats.invalid_records > 0:
        log(f"{context_prefix}Principais problemas encontrados:")
        for error in stats.validation_errors[:3]:
            log(f"{context_prefix}  - {error}")

        if len(stats.validation_errors) > 3:
            log(f"{context_prefix}  ... e mais {len(stats.validation_errors) - 3} erros")

    log(f"{context_prefix}=== FIM DO RESUMO ===")


def validate_sf_dataframe(df: pd.DataFrame, campaign_name: str) -> pd.DataFrame:
    """
    Valida que o DataFrame retornado pela query possui as colunas obrigatórias
    para o flow SF: cpf e telefone.

    Deve ser chamada logo após a checagem de df vazio no flow, antes de qualquer
    processamento. Se alguma coluna obrigatória estiver ausente, lança ValueError
    que interrompe o flow imediatamente.

    Args:
        df: DataFrame retornado pela query do BigQuery
        campaign_name: Nome da campanha (validado como não-vazio)

    Returns:
        O próprio DataFrame sem modificações

    Raises:
        ValueError: Se cpf, telefone ou campaign_name estiverem ausentes/inválidos
    """
    required_columns = ["cpf", "telefone"]
    missing = [col for col in required_columns if col not in df.columns]

    if missing:
        raise ValueError(
            f"O DataFrame não possui as colunas obrigatórias para o log SF: {missing}. "
            f"Colunas presentes: {list(df.columns)}. "
            "Verifique se a query retorna 'cpf' e 'telefone'."
        )

    if not campaign_name or not str(campaign_name).strip():
        raise ValueError("campaign_name não pode ser vazio ou nulo.")

    # Valida uma amostra de linhas via SfDispatchRow para garantir que os dados são válidos
    # (usa dispatch_date vazio apenas para checar as outras colunas — dispatch_date é gerado depois)
    sample_size = min(5, len(df))
    sample = df.head(sample_size)
    validation_errors = []

    for i, row in sample.iterrows():
        try:
            SfDispatchRow(
                dispatch_date="2000-01-01",  # placeholder para validação estrutural
                campaign_name=str(campaign_name),
                cpf=str(row.get("cpf", "")),
                telefone=str(row.get("telefone", "")),
            )
        except ValidationError as e:
            for error in e.errors():
                validation_errors.append(f"Linha {i}: {error.get('loc', ['?'])[0]} - {error.get('msg', '')}")

    if validation_errors:
        raise ValueError(
            f"Dados inválidos nas colunas obrigatórias do DataFrame SF:\n"
            + "\n".join(validation_errors)
        )

    log(f"validate_sf_dataframe: DataFrame validado com sucesso. {len(df)} linhas, colunas={list(df.columns)}")
    return df


def validate_campaign_name(
    campaign_name: str,
    billing_project_id: str,
    bucket_name: str,
) -> Optional[str]:
    """
    Valida se campaign_name existe na tabela
    `rj-crm-registry.brutos_salesforce.jornada` na coluna `hsm.nome_hsm`.

    Args:
        campaign_name: Nome da campanha a ser validado.
        billing_project_id: Projeto GCP usado para faturamento da query.
        bucket_name: Nome do bucket GCS para carregar credenciais.

    Returns:
        campaign_name se encontrado na tabela, ou None se não encontrado.
    """
    from time import sleep  # pylint: disable=C0415

    from basedosdados import Base  # pylint: disable=E0611, E0401, C0415
    from google.cloud import bigquery  # pylint: disable=E0611, E0401, C0415

    query = f"""
        SELECT COUNT(*) AS total
        FROM `rj-crm-registry.brutos_salesforce.jornada`
        WHERE hsm.nome_hsm = '{campaign_name}'
    """

    log(f"Validando campaign_name='{campaign_name}' em rj-crm-registry.brutos_salesforce.jornada ...")

    bq_client = bigquery.Client(
        credentials=Base(bucket_name=bucket_name)._load_credentials(mode="prod"),
        project=billing_project_id,
    )
    job = bq_client.query(query)
    while not job.done():
        sleep(1)

    results = list(job.result())
    total = results[0]["total"] if results else 0

    if total == 0:
        message = f"""
            ATENÇÃO: campaign_name='{campaign_name}' não encontrado na coluna hsm.nome_hsm "
            "da tabela rj-crm-registry.brutos_salesforce.jornada. "
            "Verifique o nome da campanha e tente novamente. Encerrando o flow."
        """
        log(message)
        webhook_url = os.getenv("DISCORD_WEBHOOK_URL_ERRORS")
        if not webhook_url:
            print("DISCORD_WEBHOOK_URL_ERRORS environment variable not set. Cannot send notification.")
        else:
            send_discord_notification(webhook_url, message)
        return None

    log(f"campaign_name='{campaign_name}' validado com sucesso ({total} registro(s) encontrado(s)).")
    return campaign_name


def validate_single_destination(destination_dict: Dict) -> Tuple[bool, DestinationInput, str]:
    """
    Valida um único destinatário

    Args:
        destination_dict: Dicionário com dados do destinatário

    Returns:
        Tupla contendo:
        - Bool indicando se é válido
        - DestinationInput validado (ou None se inválido)
        - String com mensagem de erro (vazia se válido)
    """
    try:
        validated = DestinationInput(**destination_dict)
        return True, validated, ""

    except ValidationError as e:
        error_details = []
        for error in e.errors():
            field = error.get("loc", ["unknown"])[0]
            message = error.get("msg", "Erro desconhecido")
            error_details.append(f"{field}: {message}")

        error_msg = "; ".join(error_details)
        return False, None, error_msg

    except Exception as e:
        return False, None, f"Erro inesperado: {e!s}"
