import json
from prefect import task
from iplanrio.pipelines_utils.logging import log
from iplanrio.pipelines_utils.env import getenv_or_action
from pipelines.rj_smas__disparo_pic.core.api_handler import ApiHandler


@task
def send_to_api(
    destinations: list[dict],
    id_hsm: int,
    campaign_name: str,
    cost_center_id: int,
    infisical_secret_path: str = "/wetalkie",
    dispatch_route: str = "dispatch",
    login_route: str = "users/login",
):
    """
    Envia os disparos via REST usando ApiHandler.

    Credenciais são obtidas de variáveis de ambiente injetadas via Infisical:
    - wetalkie_url: URL base da API WeTalkie
    - wetalkie_user: Username para autenticação
    - wetalkie_pass: Password para autenticação

    Args:
        destinations: Lista de destinatários no formato esperado pela API
        id_hsm: ID do template HSM para envio
        campaign_name: Nome da campanha
        cost_center_id: ID do centro de custo
        infisical_secret_path: Path no Infisical (para documentação)
        dispatch_route: Rota da API para dispatch
        login_route: Rota da API para login

    Returns:
        dict: Resposta da API ou dict vazio se sem conteúdo

    Raises:
        RuntimeError: Se a API retornar status >= 400
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

    # Cria payload no formato esperado pelo endpoint
    payload = {
        "id_hsm": id_hsm,
        "campaign_name": campaign_name,
        "cost_center_id": cost_center_id,
        "destinations": destinations,
    }

    # Envia o POST autenticado
    response = api.post(dispatch_route, json=payload)

    # Opcionalmente, você pode validar o resultado sem log
    if response.status_code >= 400:
        raise RuntimeError(
            f"Dispatch API returned {response.status_code}: {response.text}"
        )

    return response.json() if response.text else {}
