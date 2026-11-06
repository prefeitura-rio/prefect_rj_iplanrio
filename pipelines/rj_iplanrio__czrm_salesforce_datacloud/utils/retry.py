"""Condição de retry para tasks que chamam a API do Salesforce.

Retenta apenas erros transitórios: 5xx, 429, timeout e queda de conexão.
Erros de configuração ou lógica (``ValueError``, ``RuntimeError``, etc.)
falham imediatamente — tentar de novo não resolve.
"""

from __future__ import annotations

import requests

STATUS_ERRO_SERVIDOR = 500
STATUS_MUITAS_REQUISICOES = 429
ERROS_TRANSITORIOS = (requests.Timeout, requests.ConnectionError)


def condicao_retry(_task, _task_run, state) -> bool:
    """Retorna ``True`` se o erro for transitório e justificar nova tentativa.

    Usada como ``retry_condition_fn`` no decorator ``@task``.
    """
    try:
        state.result()
    except Exception as exc:
        causa: BaseException | None = exc
        # Desce a cadeia de causas até encontrar um erro de rede/HTTP ou o fim.
        while causa is not None and not isinstance(causa, (*ERROS_TRANSITORIOS, requests.HTTPError)):
            causa = causa.__cause__

        if isinstance(causa, requests.HTTPError):
            if causa.response is None:
                return True
            status = causa.response.status_code
            return status >= STATUS_ERRO_SERVIDOR or status == STATUS_MUITAS_REQUISICOES

        return isinstance(causa, ERROS_TRANSITORIOS)
    return False
