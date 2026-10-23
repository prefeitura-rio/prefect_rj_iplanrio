"""Condição de retry das tasks que chamam a API do Salesforce.

Só retenta o que pode passar sozinho: 5xx, 429, timeout e queda de conexão.
4xx (SQL inválida, credencial recusada, tabela inexistente) falha na hora —
tentar de novo não resolve e, com ``concurrency_limit=1``, segura a fila.
"""

from __future__ import annotations

import requests

_MIN_ERRO_SERVIDOR = 500
_MUITAS_REQUISICOES = 429


def so_erro_temporario(_task, _task_run, state) -> bool:
    """``retry_condition_fn`` do Prefect: ``True`` retenta, ``False`` falha já."""
    try:
        state.result()
    except Exception as exc:
        erro: BaseException | None = exc
        # as tasks embrulham o HTTPError num RuntimeError (raise ... from exc)
        while erro is not None and not isinstance(erro, requests.HTTPError):
            erro = erro.__cause__
        if erro is None or erro.response is None:
            return True
        return erro.response.status_code >= _MIN_ERRO_SERVIDOR or erro.response.status_code == _MUITAS_REQUISICOES
    return False
