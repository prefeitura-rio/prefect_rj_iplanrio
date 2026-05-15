# -*- coding: utf-8 -*-
"""Tasks para extração de dados da API CIVITAS/HxGN OnCall (Força Municipal)."""

import hashlib
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

import httpx
import pandas as pd
from iplanrio.pipelines_utils.logging import log
from iplanrio.pipelines_utils.pandas import parse_date_columns, to_partitions
from prefect import task

from pipelines.rj_segur__forca_municipal import env

SP_TZ = ZoneInfo("America/Sao_Paulo")
PAGE_SIZE = 500


class FMApi:
    """Cliente HTTP para a API CIVITAS/HxGN OnCall (Força Municipal)."""

    def __init__(self, base_url: str, proxy_url: Optional[str] = None):
        self.base_url = base_url.rstrip("/")
        self.proxy_url = proxy_url
        self.token: Optional[str] = None

    def _client(self) -> httpx.Client:
        kwargs: dict = {"verify": False, "timeout": 60.0}
        if self.proxy_url:
            kwargs["proxy"] = self.proxy_url
        return httpx.Client(**kwargs)

    @property
    def _headers(self) -> dict:
        if not self.token:
            raise ValueError("Not authenticated. Call authenticate() first.")
        return {"Authorization": f"Bearer {self.token}"}

    def authenticate(self, username: str, password: str) -> None:
        """Autentica na API, armazena o token e guarda as credenciais para re-auth."""
        self._username = username
        self._password = password
        with self._client() as client:
            resp = client.post(
                f"{self.base_url}/api/login",
                json={"userName": username, "password": password},
            )
            resp.raise_for_status()
            self.token = resp.json()["accessToken"]
        log("Authenticated successfully.")

    def _reauth(self) -> None:
        """Re-autentica usando as credenciais armazenadas."""
        if not hasattr(self, "_username"):
            raise RuntimeError(
                "Cannot re-authenticate: credentials not stored. Call authenticate() first."
            )
        log("Token expired (401). Re-authenticating...")
        self.authenticate(self._username, self._password)
        log("Re-authentication successful.")

    def _get_page(
        self, client: httpx.Client, path: str, page: int, page_size: int
    ) -> httpx.Response:
        """Faz GET de uma página, re-autentica e tenta novamente em caso de 401."""
        resp = client.get(
            f"{self.base_url}{path}",
            headers=self._headers,
            params={"page": page, "pageSize": page_size},
        )
        if resp.status_code == 401:
            self._reauth()
            resp = client.get(
                f"{self.base_url}{path}",
                headers=self._headers,
                params={"page": page, "pageSize": page_size},
            )
        resp.raise_for_status()
        return resp

    def get_paginated(self, path: str, page_size: int = PAGE_SIZE) -> list:
        """Busca todas as páginas de um endpoint paginado."""
        all_items: list = []
        page = 1
        while True:
            with self._client() as client:
                resp = self._get_page(client, path, page, page_size)
                body = resp.json()
            items = body.get("data", [])
            if not items:
                break
            all_items.extend(items)
            log(f"[{path}] page {page}: +{len(items)} items (total: {len(all_items)})")
            total_pages = body.get("totalPages", page)
            if page >= total_pages:
                break
            page += 1
        log(f"[{path}] Done: {len(all_items)} total items.")
        return all_items


@task
def get_authenticated_api_task() -> FMApi:
    """Cria e autentica o cliente da API Força Municipal."""
    use_proxy = env.API_FORCA_MUNICIPAL__USE_PROXY_URL
    proxy_url = env.API_FORCA_MUNICIPAL__PROXY_URL if use_proxy else None
    log(f"Proxy enabled: {use_proxy}")
    api = FMApi(base_url=env.API_FORCA_MUNICIPAL__API_URL, proxy_url=proxy_url)
    api.authenticate(
        username=env.API_FORCA_MUNICIPAL__API_LOGIN,
        password=env.API_FORCA_MUNICIPAL__API_PASSWORD,
    )
    return api


@task
def fetch_endpoint_task(
    api: FMApi, endpoint: str, page_size: int = PAGE_SIZE
) -> pd.DataFrame:
    """Busca todos os registros de um endpoint paginado e retorna um DataFrame."""
    log(f"Fetching: {endpoint}")
    items = api.get_paginated(path=endpoint, page_size=page_size)
    if not items:
        log(f"No data returned from {endpoint}", level="warning")
        return pd.DataFrame()
    df = pd.DataFrame(items)
    log(f"Shape: {df.shape}")
    return df


def _content_hash(df: pd.DataFrame) -> str:
    """Gera um hash MD5 truncado (12 chars) do conteúdo bruto do DataFrame."""
    raw = pd.util.hash_pandas_object(df, index=False).values.tobytes()
    return hashlib.md5(raw).hexdigest()[:12]


@task
def save_partitions_task(df: pd.DataFrame, table_id: str) -> str:
    """
    Adiciona updated_at e id_hash ao DataFrame, salva em partições Hive (parquet)
    particionadas pela data de updated_at.

    Fluxo:
      1. Calcula hash MD5 do conteúdo bruto da API (antes de qualquer coluna extra).
      2. Adiciona updated_at (datetime America/São Paulo) e id_hash ao DataFrame.
      3. Particiona por data de updated_at e salva como parquet com id_hash como sufixo.

    Estrutura de arquivo:
      <savepath>/ano_particao=<Y>/mes_particao=<M>/data_particao=<D>/data_<hash>.parquet

    Mesmo conteúdo da API → mesmo id_hash → mesmo nome de arquivo no GCS → overwrite
    sem duplicata. Dados diferentes → novo arquivo na partição → histórico de estados.

    Retorna o caminho raiz das partições.
    """
    content_hash = _content_hash(df)
    log(f"Content hash: {content_hash}")

    now = datetime.now(tz=SP_TZ).replace(tzinfo=None)
    df = df.copy()
    df["id_hash"] = content_hash
    df["updated_at"] = now

    savepath = f"/tmp/rj_segur__forca_municipal/{table_id}/{uuid.uuid4()}"
    Path(savepath).mkdir(parents=True, exist_ok=True)
    df, partition_cols = parse_date_columns(df, "updated_at")
    to_partitions(
        data=df,
        partition_columns=partition_cols,
        savepath=savepath,
        data_type="parquet",
        suffix=content_hash,
    )
    log(f"Partitions saved to: {savepath}")
    return savepath
