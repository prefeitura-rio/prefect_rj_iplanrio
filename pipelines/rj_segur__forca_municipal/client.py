# -*- coding: utf-8 -*-
"""Cliente HTTP para a API CIVITAS/HxGN OnCall (Força Municipal).

Pool de conexões persistente, retry exponencial com jitter e re-auth em 401.
Deve ser usado como context manager:

    async with FMApi() as api:
        items = await api.get_paginated("/api/ocorrencias/ativas", page_size=500)
"""

import asyncio
from typing import Optional

import httpx
from iplanrio.pipelines_utils.logging import log
from tenacity import (
    RetryCallState,
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential_jitter,
)

from pipelines.rj_segur__forca_municipal import env
from pipelines.rj_segur__forca_municipal.constants import (
    API_CONNECT_TIMEOUT,
    API_TIMEOUT,
    DEFAULT_PAGE_SIZE,
    MAX_PAGE_RETRIES,
    MAX_RETRIES,
    RETRY_MAX_WAIT,
    RETRY_MIN_WAIT,
)


def _is_retryable(exc: BaseException) -> bool:
    """Retenta em falhas de rede e erros HTTP 5xx (servidor indisponível)."""
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code >= 500
    return isinstance(exc, (httpx.ConnectError, httpx.TimeoutException))


def _log_retry(retry_state: RetryCallState) -> None:
    exc = retry_state.outcome.exception() if retry_state.outcome else None
    args = retry_state.args  # (self, method, path)
    self_ = args[0] if len(args) > 0 else None
    method = args[1] if len(args) > 1 else "?"
    path = args[2] if len(args) > 2 else "?"
    params = retry_state.kwargs.get("params")
    base_url = getattr(self_, "_base_url", "?")
    params_str = f" params={params}" if params else ""
    log(
        f"Tentativa {retry_state.attempt_number}/{MAX_RETRIES} falhou"
        f" ({type(exc).__name__}: {exc})"
        f" — {method} {base_url}{path}{params_str} — retentando...",
        level="warning",
    )


class FMApi:
    """Cliente HTTP auto-contido: pool persistente, retry e re-auth em 401."""

    __slots__ = (
        "_base_url",
        "_proxy_url",
        "_username",
        "_password",
        "_token",
        "_lock",
        "_client",
    )

    def __init__(self) -> None:
        self._base_url: str = (env.API_FORCA_MUNICIPAL__API_URL or "").rstrip("/")
        self._proxy_url: Optional[str] = (
            env.API_FORCA_MUNICIPAL__PROXY_URL
            if env.API_FORCA_MUNICIPAL__USE_PROXY_URL
            else None
        )
        self._username: Optional[str] = env.API_FORCA_MUNICIPAL__API_LOGIN
        self._password: Optional[str] = env.API_FORCA_MUNICIPAL__API_PASSWORD
        self._token: Optional[str] = None
        self._lock: asyncio.Lock = asyncio.Lock()
        self._client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self) -> "FMApi":
        self._client = httpx.AsyncClient(
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
            proxy=self._proxy_url,
            timeout=httpx.Timeout(API_TIMEOUT, connect=API_CONNECT_TIMEOUT),
            verify=False,
        )
        log(f"Conectando à API: {self._base_url}")
        await self._authenticate()
        return self

    async def __aexit__(self, *_) -> None:
        if self._client:
            await self._client.aclose()

    async def _authenticate(self) -> None:
        assert self._client is not None
        resp = await self._client.post(
            f"{self._base_url}/api/login",
            json={"userName": self._username, "password": self._password},
        )
        resp.raise_for_status()
        self._token = resp.json()["accessToken"]
        log(f"Autenticado em {self._base_url}")

    def _headers(self) -> dict:
        if not self._token:
            raise RuntimeError("FMApi não autenticado. Use como context manager.")
        return {"Authorization": f"Bearer {self._token}"}

    async def _reauth(self, stale_token: Optional[str]) -> None:
        """Re-autentica apenas se o token ainda for o que causou o 401.

        O lock garante que apenas uma coroutine re-autentica por vez,
        evitando thundering herd quando múltiplas requests recebem 401.
        """
        async with self._lock:
            if self._token == stale_token:
                log("Token expirado (401) — re-autenticando...", level="warning")
                await self._authenticate()

    @retry(
        stop=stop_after_attempt(MAX_RETRIES),
        wait=wait_exponential_jitter(initial=RETRY_MIN_WAIT, max=RETRY_MAX_WAIT),
        retry=retry_if_exception(_is_retryable),
        before_sleep=_log_retry,
        reraise=True,
    )
    async def _request(self, method: str, path: str, **kwargs) -> httpx.Response:
        """Request com retry em falhas de rede e re-auth em 401."""
        assert self._client is not None
        url = f"{self._base_url}{path}"
        stale_token = self._token
        resp = await self._client.request(
            method, url, headers=self._headers(), **kwargs
        )
        if resp.status_code == 401:
            await self._reauth(stale_token)
            resp = await self._client.request(
                method, url, headers=self._headers(), **kwargs
            )
        resp.raise_for_status()
        return resp

    async def get_json(self, path: str, params: Optional[dict] = None) -> dict:
        return (await self._request("GET", path, params=params)).json()

    async def get_text(self, path: str, params: Optional[dict] = None) -> str:
        return (await self._request("GET", path, params=params)).text

    async def get_paginated(
        self,
        path: str,
        page_size: int = DEFAULT_PAGE_SIZE,
        extra_params: Optional[dict] = None,
        sem: Optional[asyncio.Semaphore] = None,
        max_page_retries: int = MAX_PAGE_RETRIES,
    ) -> list[dict]:
        """
        Busca página 1, descobre totalPages, paraleliza o restante.

        Páginas que falham após os retries do _request são coletadas e
        reagrupadas em rounds adicionais (max_page_retries). Se ao final
        ainda houver páginas com falha, levanta RuntimeError — nunca
        retorna resultado parcial silenciosamente.
        """
        ctx = sem or asyncio.Semaphore(100)
        full_url = f"{self._base_url}{path}"

        extra_str = (
            ", ".join(f"{k}={v}" for k, v in extra_params.items())
            if extra_params
            else ""
        )

        params: dict = {"page": 1, "pageSize": page_size}
        if extra_params:
            params.update(extra_params)

        async with ctx:
            first = await self.get_json(path, params=params)

        items: list[dict] = list(first.get("data", []))
        total_pages: int = first.get("totalPages", 1)

        extra_log = f", {extra_str}" if extra_str else ""
        log(
            f"GET {full_url}"
            f" [page=1/{total_pages}, pageSize={page_size}{extra_log}]"
            f" → {len(items)} itens"
        )

        if total_pages <= 1:
            return items

        log(f"GET {full_url} — {total_pages} páginas, buscando em paralelo...")

        async def _fetch(page: int) -> list[dict]:
            p: dict = {"page": page, "pageSize": page_size}
            if extra_params:
                p.update(extra_params)
            async with ctx:
                body = await self.get_json(path, params=p)
            page_items: list[dict] = body.get("data", [])
            log(
                f"GET {full_url}"
                f" [page={page}/{total_pages}, pageSize={page_size}{extra_log}]"
                f" → {len(page_items)} itens"
            )
            return page_items

        pages_map: dict[int, list[dict]] = {}
        pending: set[int] = set(range(2, total_pages + 1))

        for attempt in range(max_page_retries + 1):
            if not pending:
                break

            if attempt > 0:
                log(
                    f"GET {full_url} — retentando {len(pending)} página(s) que falharam"
                    f" (round {attempt}/{max_page_retries})...",
                    level="warning",
                )

            outcomes = await asyncio.gather(
                *[_fetch(p) for p in sorted(pending)],
                return_exceptions=True,
            )

            still_pending: set[int] = set()
            for page, outcome in zip(sorted(pending), outcomes):
                if isinstance(outcome, Exception):
                    still_pending.add(page)
                else:
                    pages_map[page] = outcome  # type: ignore[assignment]

            pending = still_pending

        if pending:
            raise RuntimeError(
                f"GET {full_url} — {len(pending)} página(s) falharam definitivamente"
                f" após {max_page_retries + 1} tentativa(s): páginas {sorted(pending)}"
            )

        for page in range(2, total_pages + 1):
            items.extend(pages_map.pop(page))

        log(f"GET {full_url} — total: {len(items)} itens em {total_pages} página(s)")
        return items
