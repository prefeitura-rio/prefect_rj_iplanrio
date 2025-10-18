# -*- coding: utf-8 -*-
"""
Tasks para disparo em lotes via API WeTalkie - SMAS Disparo PIC
"""

from datetime import datetime
from math import ceil

from iplanrio.pipelines_utils.env import getenv_or_action
from iplanrio.pipelines_utils.logging import log
from prefect import task
from pytz import timezone

from pipelines.rj_smas__disparo_pic.core.api_handler import ApiHandler


@task
def send_to_api(
    destinations: list[dict],
    id_hsm: int,
    campaign_name: str,
    cost_center_id: int,
    chunk_size: int = 1000,
    infisical_secret_path: str = "/wetalkie",
    dispatch_route: str = "dispatch",
    login_route: str = "users/login",
) -> str:
    """
    Envia disparos em lotes via API WeTalkie.

    Divide os destinatários em lotes (batches) e envia cada um separadamente,
    atualizando o nome da campanha com a data e número do lote.

    Credenciais são obtidas de variáveis de ambiente injetadas via Infisical:
    - wetalkie_url: URL base da API WeTalkie
    - wetalkie_user: Username para autenticação
    - wetalkie_pass: Password para autenticação

    Args:
        destinations: Lista de destinatários no formato esperado pela API
        id_hsm: ID do template HSM para envio
        campaign_name: Nome da campanha (base)
        cost_center_id: ID do centro de custo
        chunk_size: Tamanho dos lotes para envio (default: 1000)
        infisical_secret_path: Path no Infisical para as credenciais
        dispatch_route: Rota da API para dispatch
        login_route: Rota da API para login

    Returns:
        str: Data/hora do disparo no formato 'YYYY-MM-DD HH:MM:SS'

    Raises:
        RuntimeError: Se a API retornar status >= 400 ou sem destinatários
    """
    # Busca credenciais das variáveis de ambiente (injetadas via Infisical)
    base_url = getenv_or_action("wetalkie_url")
    username = getenv_or_action("wetalkie_user")
    password = getenv_or_action("wetalkie_pass")

    log(f"Connecting to WeTalkie API at {base_url}")

    # Instancia o cliente autenticado
    api = ApiHandler(
        base_url=base_url,
        username=username,
        password=password,
        login_route=login_route,
    )

    total = len(destinations)
    dispatch_date = datetime.now(timezone("America/Sao_Paulo")).strftime("%Y-%m-%d %H:%M:%S")

    if total == 0:
        raise RuntimeError("No destinations to dispatch")

    total_batches = ceil(total / chunk_size)
    log(f"Starting dispatch of {total} destinations in {total_batches} batches of size {chunk_size}")

    for i, start in enumerate(range(0, total, chunk_size), 1):
        end = start + chunk_size
        batch = destinations[start:end]

        # Append batch information to campaign name
        batch_campaign_name = f"{campaign_name}-{dispatch_date[:10]}-lote{i}"

        # Cria payload no formato esperado pelo endpoint
        payload = {
            "id_hsm": id_hsm,
            "campaign_name": batch_campaign_name,
            "cost_center_id": cost_center_id,
            "destinations": batch,
        }

        log(f"Dispatching batch {i}/{total_batches} with {len(batch)} destinations")

        # Envia o POST autenticado
        response = api.post(dispatch_route, json=payload)

        if response.status_code >= 400:
            log(f"Batch {i} dispatch failed: {response.text}")
            raise RuntimeError(f"Dispatch API batch {i} returned {response.status_code}: {response.text}")

        log(f"Batch {i} dispatched successfully")

    log(f"All {total} destinations dispatched successfully in {total_batches} batches")
    return dispatch_date
