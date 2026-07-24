# -*- coding: utf-8 -*-
"""
Pre-flight checks para a pipeline Agentforce → BigQuery.

Valida antes de cada execução:
  1. Autenticação Bulk API 2.0
  2. Autenticação Data Cloud (token válido)
  3. DMOs esperadas existem no Data Cloud (query LIMIT 1)
  4. Dataset de destino existe no BigQuery

Qualquer check crítico que falhar levanta RuntimeError e aborta o flow.
Checks não-críticos (MCE, tracing, genai) logam warning e retornam False.
"""

from __future__ import annotations

import requests
from google.cloud import bigquery
from prefect import task

_API_VERSION = "v67.0"
_QUERY_ENDPOINT = f"/services/data/{_API_VERSION}/ssot/query-sql"
_WORKLOAD = "BatchQuery"

# DMOs obrigatórias para F1 (STDM) — com prefixo ssot__ correto
_REQUIRED_DMOS_F1 = [
    "ssot__AiAgentSession__dlm",
    "ssot__AiAgentInteraction__dlm",
    "ssot__AiAgentInteractionStep__dlm",
    "ssot__AiAgentInteractionMessage__dlm",
]

# DMOs opcionais — falha vira warning, não erro
_OPTIONAL_DMOS = {
    "mce": [
        "ssot__MCE_Sent__dlm",
        "ssot__MCE_Open__dlm",
        "ssot__MCE_Click__dlm",
        "ssot__MCE_Bounce__dlm",
        "ssot__MCE_Unsub__dlm",
        "ssot__MCE_Subscriber__dlm",
    ],
    "tracing": ["ssot__TelemetryTraceSpan__dlm"],
    "genai": [
        "ssot__GenAIGatewayRequest__dlm",
        "ssot__GenAIGeneration__dlm",
        "ssot__GenAIQuality__dlm",
        "ssot__GenAIFeedback__dlm",
    ],
}


# ---------------------------------------------------------------------------
# Checks individuais (funções internas, não tasks)
# ---------------------------------------------------------------------------


def _check_bulk_api_auth(access_token: str, instance_url: str) -> bool:
    """Valida que o token da Bulk API ainda é válido fazendo um request leve."""
    url = f"{instance_url}/services/data/v59.0/limits"
    headers = {"Authorization": f"Bearer {access_token}"}
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        return True
    except Exception as exc:
        print(f"[PREFLIGHT][BULK_AUTH] FAIL: {exc}")
        return False


def _check_dc_auth(dc_session: dict) -> bool:
    """
    Valida que o token do Data Cloud é válido executando uma query mínima.
    Usa a tabela AiAgentSession que sabemos existir.
    """
    url = (
        f"{dc_session['instance_url']}{_QUERY_ENDPOINT}"
        f"?dataspace={dc_session.get('dataspace', 'default')}&workloadName={_WORKLOAD}"
    )
    headers = {
        "Authorization": f"Bearer {dc_session['access_token']}",
        "Content-Type": "application/json",
    }
    try:
        resp = requests.post(
            url,
            headers=headers,
            json={"sql": "SELECT ssot__Id__c FROM ssot__AiAgentSession__dlm LIMIT 1"},
            timeout=20,
        )
        resp.raise_for_status()
        return True
    except Exception as exc:
        print(f"[PREFLIGHT][DC_AUTH] FAIL: {exc}")
        return False


def _check_dc_table_exists(dc_session: dict, table_name: str) -> bool:
    """
    Verifica se um DMO existe no Data Cloud tentando SELECT LIMIT 1.
    Retorna True se existir, False se não.
    """
    url = (
        f"{dc_session['instance_url']}{_QUERY_ENDPOINT}"
        f"?dataspace={dc_session.get('dataspace', 'default')}&workloadName={_WORKLOAD}"
    )
    headers = {
        "Authorization": f"Bearer {dc_session['access_token']}",
        "Content-Type": "application/json",
    }
    try:
        resp = requests.post(
            url,
            headers=headers,
            json={"sql": f"SELECT * FROM {table_name} LIMIT 1"},
            timeout=20,
        )
        if resp.ok:
            return True
        body = resp.text.lower()
        if "does not exist" in body or "not found" in body:
            return False
        resp.raise_for_status()
        return True
    except requests.HTTPError:
        return False
    except Exception as exc:
        print(f"[PREFLIGHT] WARN: erro ao verificar '{table_name}': {exc}")
        return False


def _check_bq_dataset(project_id: str, dataset_id: str) -> bool:
    """Verifica se o dataset de destino existe no BigQuery."""
    try:
        client = bigquery.Client(project=project_id)
        client.get_dataset(dataset_id)
        print(f"[PREFLIGHT][BQ] OK — dataset '{dataset_id}' existe.")
        return True
    except Exception as exc:
        print(f"[PREFLIGHT][BQ] FAIL — dataset '{dataset_id}' nao encontrado: {exc}")
        return False


# ---------------------------------------------------------------------------
# Task principal
# ---------------------------------------------------------------------------


@task(log_prints=True)
def run_preflight_checks(
    bulk_session: dict,
    dc_session: dict,
    bq_project_id: str,
    bq_dataset_id: str,
) -> dict[str, bool]:
    """
    Executa todos os pre-flight checks e retorna um dict com resultados.

    Checks críticos (levantam RuntimeError se falharem):
      - bulk_auth    : token Bulk API válido
      - bq_dataset   : dataset BigQuery existe
      - dmos_f1      : todas as DMOs de F1 existem no Data Cloud

    Checks não-críticos (retornam False, flow decide o que fazer):
      - dc_auth      : token Data Cloud válido
      - dmos_mce     : DMOs de MCE disponíveis
      - dmos_tracing : DMO de Platform Tracing disponível
      - dmos_genai   : DMOs de GenAI Audit disponíveis

    Args:
        bulk_session : dict com 'access_token' e 'instance_url' (de get_bulk_api_session).
        dc_session   : dict com 'access_token', 'instance_url', 'dataspace'
                       (de get_data_cloud_session).
        bq_project_id: ID do projeto GCP.
        bq_dataset_id: ID do dataset BigQuery de destino.

    Returns:
        dict com resultado booleano de cada check.

    Raises:
        RuntimeError: Se qualquer check crítico falhar.
    """
    results: dict[str, bool] = {}

    print("[PREFLIGHT] === Iniciando pre-flight checks ===")

    # --- Crítico: Bulk API auth ---
    print("[PREFLIGHT] Verificando autenticacao Bulk API...")
    results["bulk_auth"] = _check_bulk_api_auth(
        access_token=bulk_session["access_token"],
        instance_url=bulk_session["instance_url"],
    )
    if not results["bulk_auth"]:
        raise RuntimeError("[PREFLIGHT] CRITICO: autenticacao Bulk API invalida. Abortando.")

    # --- Crítico: BigQuery dataset ---
    print(f"[PREFLIGHT] Verificando dataset BigQuery '{bq_dataset_id}'...")
    results["bq_dataset"] = _check_bq_dataset(
        project_id=bq_project_id,
        dataset_id=bq_dataset_id,
    )
    if not results["bq_dataset"]:
        raise RuntimeError(
            f"[PREFLIGHT] CRITICO: dataset '{bq_dataset_id}' nao existe no BigQuery. "
            "Crie o dataset antes de executar a pipeline."
        )

    # --- Não-crítico: Data Cloud auth ---
    print("[PREFLIGHT] Verificando autenticacao Data Cloud...")
    results["dc_auth"] = _check_dc_auth(dc_session)
    if not results["dc_auth"]:
        print("[PREFLIGHT] WARN: token Data Cloud invalido — fases DC serao puladas.")

    # --- Crítico: DMOs de F1 ---
    if results["dc_auth"]:
        print("[PREFLIGHT] Verificando DMOs obrigatorias (F1 — STDM)...")
        missing_f1 = []
        for dmo in _REQUIRED_DMOS_F1:
            exists = _check_dc_table_exists(dc_session, dmo)
            print(f"[PREFLIGHT]   {dmo}: {'OK' if exists else 'NAO ENCONTRADA'}")
            if not exists:
                missing_f1.append(dmo)
        results["dmos_f1"] = len(missing_f1) == 0
        if missing_f1:
            raise RuntimeError(
                f"[PREFLIGHT] CRITICO: DMOs ausentes no Data Cloud: {missing_f1}. "
                "Verifique se o Data Cloud esta configurado e as licencas ativas."
            )

        # --- Não-crítico: DMOs MCE ---
        print("[PREFLIGHT] Verificando DMOs MCE...")
        mce_ok = all(_check_dc_table_exists(dc_session, t) for t in _OPTIONAL_DMOS["mce"])
        results["dmos_mce"] = mce_ok
        if not mce_ok:
            print("[PREFLIGHT] WARN: DMOs MCE ausentes — Fase 2b (MCE) sera pulada.")

        # --- Não-crítico: Tracing ---
        print("[PREFLIGHT] Verificando DMO Platform Tracing...")
        tracing_ok = all(
            _check_dc_table_exists(dc_session, t) for t in _OPTIONAL_DMOS["tracing"]
        )
        results["dmos_tracing"] = tracing_ok
        if not tracing_ok:
            print("[PREFLIGHT] WARN: DMO TelemetryTraceSpan ausente — Fase 3 sera pulada.")

        # --- Não-crítico: GenAI ---
        print("[PREFLIGHT] Verificando DMOs GenAI Audit...")
        genai_ok = all(
            _check_dc_table_exists(dc_session, t) for t in _OPTIONAL_DMOS["genai"]
        )
        results["dmos_genai"] = genai_ok
        if not genai_ok:
            print("[PREFLIGHT] WARN: DMOs GenAI ausentes — Fase 4 sera pulada.")
    else:
        results.update(
            dmos_f1=False, dmos_mce=False, dmos_tracing=False, dmos_genai=False
        )

    print(f"[PREFLIGHT] === Resultados: {results} ===")
    return results
