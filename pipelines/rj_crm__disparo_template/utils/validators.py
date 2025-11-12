# -*- coding: utf-8 -*-
# flake8: noqa:E501
# pylint: disable='line-too-long'

"""
Funções centralizadas de validação para pipeline SMAS Disparo CADUNICO
Implementa validação robusta com logs detalhados e métricas de qualidade
"""

from typing import Dict, List, Tuple

from iplanrio.pipelines_utils.logging import log  # pylint: disable=E0611, E0401
from pydantic import ValidationError

# pylint: disable=E0611, E0401
from pipelines.rj_crm__disparo_template.utils.schemas import (
    DestinationInput,
    DispatchPayload,
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
        raise ValidationError(error_msg)


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
