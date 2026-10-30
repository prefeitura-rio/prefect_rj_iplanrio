"""Condição de retry das tasks que chamam a API do Salesforce.

Só retenta o que pode passar sozinho: 5xx, 429, timeout e queda de conexão
(``requests.Timeout``/``requests.ConnectionError``). Qualquer outra exceção —
inclusive ``ValueError``/``KeyError``/``RuntimeError`` de configuração ou de
SQL — falha na hora: tentar de novo não resolve e, com ``concurrency_limit=1``,
3 tentativas com 30/60/120s seguram a fila por ~3,5 min.
"""

from __future__ import annotations

import requests

_MIN_ERRO_SERVIDOR = 500
_MUITAS_REQUISICOES = 429
_TRANSITORIOS = (requests.Timeout, requests.ConnectionError)


def so_erro_temporario(_task, _task_run, state) -> bool:
    """``retry_condition_fn`` do Prefect: ``True`` retenta, ``False`` falha já."""
    try:
        state.result()
    except Exception as exc:
        erro: BaseException | None = exc
        # as tasks embrulham o HTTPError num RuntimeError (raise ... from exc);
        # desce a cadeia até achar um erro de rede/HTTP ou o fim dela
        while erro is not None and not isinstance(erro, (*_TRANSITORIOS, requests.HTTPError)):
            erro = erro.__cause__

        if isinstance(erro, requests.HTTPError):
            if erro.response is None:
                return True
            status = erro.response.status_code
            return status >= _MIN_ERRO_SERVIDOR or status == _MUITAS_REQUISICOES
        return isinstance(erro, _TRANSITORIOS)
    return False
