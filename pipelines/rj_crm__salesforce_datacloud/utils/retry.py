# -*- coding: utf-8 -*-
"""
Condição de retry das tasks que chamam a API da Salesforce.

Só retenta erro que pode passar sozinho: 5xx, 429 (rate limit), timeout,
queda de conexão. Erro 4xx (tabela/coluna inexistente, SQL/SOQL inválida,
credencial recusada) falha na hora — tentar de novo nunca resolve, e com
retries=3 e espera de 30/60/120s uma tabela quebrada segurava o run ~3,5min
(e, com concurrency_limit=1, os ticks seguintes na fila). Medido em
2026-09-29 ao tirar o pre-flight, que antes pulava tabela inexistente.
"""

from __future__ import annotations

import requests


def so_erro_temporario(task, task_run, state) -> bool:
    """retry_condition_fn do Prefect: True = retenta, False = falha já."""
    try:
        state.result()
    except Exception as exc:  # noqa: BLE001 — é justamente o erro da task
        erro: BaseException | None = exc
        # extract_* embrulham o HTTPError num RuntimeError (raise ... from exc)
        while erro is not None and not isinstance(erro, requests.HTTPError):
            erro = erro.__cause__
        if erro is None or erro.response is None:
            return True  # timeout, conexão, etc. — pode ser passageiro
        status = erro.response.status_code
        return status >= 500 or status == 429
    return False
