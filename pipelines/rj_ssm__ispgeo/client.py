"""Cliente HTTP para o portal ArcGIS do ISP-GEO (Instituto de Segurança Pública).

Autentica via token e consulta a camada de microdados de ocorrências
(Microdados_PCERJ). Deve ser usado como context manager:

    with IspGeoClient() as client:
        domains = client.get_field_domains()
"""

import asyncio
from types import TracebackType

import aiohttp
import requests
from iplanrio.pipelines_utils.env import getenv_or_action

from pipelines.rj_ssm__ispgeo.constants import (
    AUTH_TIMEOUT,
    COUNT_TIMEOUT,
    DEFAULT_PAGE_SIZE,
    DOMAIN_TIMEOUT,
    MAX_CONCURRENT_REQUESTS,
    QUERY_TIMEOUT,
)
from prefect_rj_iplanrio.logging import get_logger

logger = get_logger(__name__)


class IspGeoClient:
    """Cliente autocontido para o portal ArcGIS do ISP-GEO."""

    __slots__ = ("username", "password", "portal_url", "layer_url", "referer", "token", "session")

    def __init__(self) -> None:
        """Inicializa o cliente com credenciais lidas das variáveis de ambiente."""
        self.username: str | None = getenv_or_action(key="ISPGEO_USER", action="ignore")
        self.password: str | None = getenv_or_action(key="ISPGEO_PASS", action="ignore")
        self.portal_url: str | None = getenv_or_action(key="ISPGEO_PORTAL_URL", action="ignore")
        self.layer_url: str | None = getenv_or_action(key="ISPGEO_LAYER_URL", action="ignore")
        self.referer: str | None = getenv_or_action(key="ISPGEO_REFERER", action="ignore")
        self.token: str | None = None
        self.session: requests.Session | None = None

    def __enter__(self) -> "IspGeoClient":
        """Abre a sessão HTTP e autentica no portal.

        :returns: A própria instância, autenticada.
        """
        self.session = requests.Session()
        self.token = self.authenticate()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Fecha a sessão HTTP."""
        if self.session:
            self.session.close()

    def authenticate(self) -> str:
        """Autentica no portal ArcGIS e retorna o token de acesso.

        :returns: Token de acesso válido.
        :raises RuntimeError: Se a resposta não contiver um token.
        """
        assert self.session is not None
        resp = self.session.post(
            self.portal_url,
            data={
                "username": self.username,
                "password": self.password,
                "referer": self.referer,
                "f": "json",
            },
            timeout=AUTH_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        if "token" not in data:
            raise RuntimeError(f"Falha ao autenticar no ISP-GEO: {data}")
        logger.info("Autenticado no portal ISP-GEO.")
        return data["token"]

    def get_field_domains(self) -> dict[str, dict[int, str]]:
        """Baixa os domínios (código -> nome) de cada campo codificado da camada.

        :returns: Mapa de nome do campo para o mapa código -> nome do domínio.
        """
        assert self.session is not None
        resp = self.session.get(
            self.layer_url, params={"f": "json", "token": self.token}, timeout=DOMAIN_TIMEOUT
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
        assert self.session is not None
        resp = self.session.get(
            f"{self.layer_url}/query",
            params={
                "where": where,
                "returnCountOnly": "true",
                "f": "json",
                "token": self.token,
            },
            timeout=COUNT_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        if "error" in data:
            raise RuntimeError(f"Erro ao contar registros: {data['error']}")

        return data["count"]

    async def fetch_page_async(
        self,
        session: aiohttp.ClientSession,
        sem: asyncio.Semaphore,
        where: str,
        offset: int,
        page_size: int,
        total: int,
        page_num: int,
        total_pages: int,
    ) -> list[dict]:
        """Busca uma única página de features de forma assíncrona.

        :param session: Sessão aiohttp compartilhada.
        :param sem: Semáforo que limita requisições simultâneas.
        :param where: Cláusula ``WHERE`` no dialeto SQL da API ArcGIS.
        :param offset: Índice do primeiro registro da página.
        :param page_size: Quantidade de registros por página.
        :param total: Total de registros esperados, usado para validar o tamanho da página.
        :param page_num: Número ordinal desta página (1-based).
        :param total_pages: Total de páginas da requisição.
        :returns: Lista de dicts de atributos da página.
        :raises RuntimeError: Se a API retornar um erro ou se a página vier com
            menos registros do que o esperado (ex.: ``maxRecordCount`` da camada
            menor que ``page_size``).
        """
        url = f"{self.layer_url}/query"
        params = {
            "where": where,
            "outFields": "*",
            "returnGeometry": "false",
            "resultOffset": offset,
            "resultRecordCount": page_size,
            "f": "json",
            "token": self.token,
        }

        async with sem:
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
        expected = min(page_size, total - offset)
        if len(features) != expected:
            raise RuntimeError(
                f"Página incompleta (offset={offset}): "
                f"{len(features)} de {expected} registros esperados."
            )
        pct = round(page_num / total_pages * 100)
        logger.info(
            f"GET {url} [página {page_num}/{total_pages} ({pct}%), pageSize={page_size}] → {len(features)} itens"
        )
        return features

    async def fetch_all_async(
        self,
        where: str,
        total: int,
        page_size: int,
        max_concurrent_requests: int,
    ) -> list[dict]:
        """Busca todas as páginas de forma assíncrona e em paralelo.

        Limita a concorrência via semáforo, evitando sobrecarregar o servidor.

        :param where: Cláusula ``WHERE`` no dialeto SQL da API ArcGIS.
        :param total: Total de registros esperados (retornado por :meth:`count_records`).
        :param page_size: Quantidade de registros por página.
        :param max_concurrent_requests: Número máximo de requisições simultâneas.
        :returns: Lista completa de dicts de atributos, na ordem dos offsets.
        :raises RuntimeError: Se alguma página vier incompleta ou a API retornar um erro.
        """
        offsets = list(range(0, total, page_size))
        total_pages = len(offsets)
        logger.info(
            f"Buscando {total} registros em {total_pages} página(s) "
            f"(pageSize={page_size}, concorrência={max_concurrent_requests})."
        )

        sem = asyncio.Semaphore(max_concurrent_requests)
        async with aiohttp.ClientSession() as session:
            tasks = [
                self.fetch_page_async(
                    session, sem, where, offset, page_size,
                    total=total,
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
        self,
        where: str,
        page_size: int = DEFAULT_PAGE_SIZE,
        max_concurrent_requests: int = MAX_CONCURRENT_REQUESTS,
        total: int | None = None,
    ) -> list[dict]:
        """Busca todas as features que batem com ``where``, paginando de forma assíncrona.

        Obtém o total de registros via :meth:`count_records` (se ``total`` não for
        informado) e dispara todas as páginas em paralelo usando ``asyncio`` + ``aiohttp``.

        :param where: Cláusula ``WHERE`` no dialeto SQL da API ArcGIS.
        :param page_size: Quantidade de registros por página.
        :param max_concurrent_requests: Número máximo de requisições simultâneas.
        :param total: Total de registros já contado pelo chamador, para evitar uma
            segunda contagem que pode divergir da primeira.
        :returns: Lista de dicts de atributos (``attributes``) de cada feature.
        :raises RuntimeError: Se a API retornar um erro ou uma página incompleta.
        """
        if total is None:
            total = self.count_records(where=where)
        if total == 0:
            logger.info(f"Nenhum registro encontrado para o filtro informado.")
            return []

        return asyncio.run(self.fetch_all_async(
            where=where,
            total=total,
            page_size=page_size,
            max_concurrent_requests=max_concurrent_requests,
        ))
