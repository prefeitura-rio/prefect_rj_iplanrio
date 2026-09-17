"""Cliente HTTP para o portal ArcGIS do ISP-GEO (Instituto de Segurança Pública).

Autentica via token e consulta a camada de microdados de ocorrências
(Microdados_PCERJ). Deve ser usado como context manager:

    with IspGeoClient() as client:
        domains = client.get_field_domains()
"""

import asyncio
from types import TracebackType
from typing import Optional

import aiohttp
import requests
from prefect_rj_iplanrio.logging import get_logger

import env
from constants import (
    AUTH_TIMEOUT,
    COUNT_TIMEOUT,
    DEFAULT_PAGE_SIZE,
    DOMAIN_TIMEOUT,
    QUERY_TIMEOUT,
)

logger = get_logger(__name__)


class IspGeoClient:
    """Cliente autocontido para o portal ArcGIS do ISP-GEO."""

    __slots__ = ("_username", "_password", "_portal_url", "_layer_url", "_referer", "_token", "_session")

    def __init__(self) -> None:
        """Inicializa o cliente com credenciais lidas de ``env.py``."""
        self._username: Optional[str] = env.ISPGEO_USER
        self._password: Optional[str] = env.ISPGEO_PASS
        self._portal_url: Optional[str] = env.ISPGEO_PORTAL_URL
        self._layer_url: Optional[str] = env.ISPGEO_LAYER_URL
        self._referer: Optional[str] = env.ISPGEO_REFERER
        self._token: Optional[str] = None
        self._session: Optional[requests.Session] = None

    def __enter__(self) -> "IspGeoClient":
        """Abre a sessão HTTP e autentica no portal.

        :returns: A própria instância, autenticada.
        """
        self._session = requests.Session()
        self._token = self._authenticate()
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        """Fecha a sessão HTTP."""
        if self._session:
            self._session.close()

    def _authenticate(self) -> str:
        """Autentica no portal ArcGIS e retorna o token de acesso.

        :returns: Token de acesso válido.
        :raises RuntimeError: Se a resposta não contiver um token.
        """
        assert self._session is not None
        resp = self._session.post(
            self._portal_url,
            data={
                "username": self._username,
                "password": self._password,
                "referer": self._referer,
                "f": "json",
            },
            timeout=AUTH_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        if "token" not in data:
            raise RuntimeError(f"Falha ao autenticar no ISP-GEO: {data}")
        print(f"Autenticado no portal ISP-GEO.")
        return data["token"]

    def get_field_domains(self) -> dict[str, dict[int, str]]:
        """Baixa os domínios (código -> nome) de cada campo codificado da camada.

        :returns: Mapa de nome do campo para o mapa código -> nome do domínio.
        """
        assert self._session is not None
        resp = self._session.get(
            self._layer_url, params={"f": "json", "token": self._token}, timeout=DOMAIN_TIMEOUT
        )
        resp.raise_for_status()
        data = resp.json()
        domains: dict[str, dict[int, str]] = {}
        for field in data["fields"]:
            domain = field.get("domain")
            if domain and domain.get("codedValues"):
                domains[field["name"]] = {
                    cv["code"]: cv["name"] for cv in domain["codedValues"]
                }
        return domains

    def count_records(self, where: str) -> int:
        """Pede ao servidor a contagem total de registros que batem com ``where``.

        Serve para confirmar que nenhum registro foi perdido durante a extração
        (memória, timeout ou qualquer outro problema no meio do caminho).

        :param where: Cláusula ``WHERE`` no dialeto SQL da API ArcGIS.
        :returns: Total de registros no servidor para a cláusula informada.
        :raises RuntimeError: Se a API retornar um erro.
        """
        assert self._session is not None
        resp = self._session.get(
            f"{self._layer_url}/query",
            params={
                "where": where,
                "returnCountOnly": "true",
                "f": "json",
                "token": self._token,
            },
            timeout=COUNT_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        if "error" in data:
            raise RuntimeError(f"Erro ao contar registros: {data['error']}")

        return data["count"]

    async def _fetch_page_async(
        self,
        session: aiohttp.ClientSession,
        where: str,
        offset: int,
        page_size: int,
        page_num: int,
        total_pages: int,
    ) -> list[dict]:
        """Busca uma única página de features de forma assíncrona.

        :param session: Sessão aiohttp compartilhada.
        :param where: Cláusula ``WHERE`` no dialeto SQL da API ArcGIS.
        :param offset: Índice do primeiro registro da página.
        :param page_size: Quantidade de registros por página.
        :param page_num: Número ordinal desta página (1-based).
        :param total_pages: Total de páginas da requisição.
        :returns: Lista de dicts de atributos da página.
        :raises RuntimeError: Se a API retornar um erro.
        """
        url = f"{self._layer_url}/query"
        params = {
            "where": where,
            "outFields": "*",
            # "orderByFields": "class_geocode DESC, datf ASC",
            "returnGeometry": "false",
            "resultOffset": offset,
            "resultRecordCount": page_size,
            "f": "json",
            "token": self._token,
        }

        async with session.get(
            url,
            params=params,
            timeout=aiohttp.ClientTimeout(total=QUERY_TIMEOUT),
        ) as resp:

            resp.raise_for_status()
            data = await resp.json(content_type=None)

        if "error" in data:
            raise RuntimeError(f"Erro na consulta ao ISP-GEO (offset={offset}): {data['error']}")

        features = [f["attributes"] for f in data.get("features", [])]
        pct = round(page_num / total_pages * 100)
        print(
            f"[página {page_num}/{total_pages}"
            f"({pct}%), pageSize={page_size}] → {len(features)} itens"
        )


        return features

    async def _fetch_all_async(
        self,
        where: str,
        total: int,
        page_size: int,
    ) -> list[dict]:
        """Busca todas as páginas de forma assíncrona e em paralelo.

        Divide o total de registros em offsets e dispara todas as requisições
        simultaneamente usando ``aiohttp``.

        :param where: Cláusula ``WHERE`` no dialeto SQL da API ArcGIS.
        :param total: Total de registros esperados (retornado por :meth:`count_records`).
        :param page_size: Quantidade de registros por página.
        :returns: Lista completa de dicts de atributos, na ordem dos offsets.
        """
        offsets = list(range(0, total, page_size))
        total_pages = len(offsets)
        print(
            f"Buscando {total} registros em {total_pages} página(s) assíncronas (pageSize={page_size})."
        )

        async with aiohttp.ClientSession() as session:
            tasks = [
                self._fetch_page_async(
                    session, where, offset, page_size,
                    page_num=i + 1,
                    total_pages=total_pages,
                )
                for i, offset in enumerate(offsets)
            ]
            pages = await asyncio.gather(*tasks)

        features: list[dict] = []
        for page in pages:
            features.extend(page)
        return features

    def fetch_features(
        self, where: str, page_size: int = DEFAULT_PAGE_SIZE
    ) -> list[dict]:
        """Busca todas as features que batem com ``where``, paginando de forma assíncrona.

        Obtém o total de registros via :meth:`count_records` e dispara todas as
        páginas em paralelo usando ``asyncio`` + ``aiohttp``.

        :param where: Cláusula ``WHERE`` no dialeto SQL da API ArcGIS.
        :param page_size: Quantidade de registros por página.
        :returns: Lista de dicts de atributos (``attributes``) de cada feature.
        :raises RuntimeError: Se a API retornar um erro em alguma página.
        """
        total = self.count_records(where=where)
        if total == 0:
            print(f"Nenhum registro encontrado para o filtro informado.")
            return []

        return asyncio.run(self._fetch_all_async(where=where, total=total, page_size=page_size))
