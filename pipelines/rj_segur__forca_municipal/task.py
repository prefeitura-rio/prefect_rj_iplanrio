# -*- coding: utf-8 -*-
"""Tasks para extração de dados da API CIVITAS/HxGN OnCall (Força Municipal)."""

import hashlib
import json
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
        log(f"Autenticado em {self.base_url}")

    def _reauth(self) -> None:
        """Re-autentica usando as credenciais armazenadas."""
        if not hasattr(self, "_username"):
            raise RuntimeError(
                "Cannot re-authenticate: credentials not stored. Call authenticate() first."
            )
        log("Token expirado (401) — re-autenticando...", level="warning")
        self.authenticate(self._username, self._password)

    def _get_page(
        self,
        client: httpx.Client,
        path: str,
        page: int,
        page_size: int,
        extra_params: Optional[dict] = None,
    ) -> httpx.Response:
        """Faz GET de uma página, re-autentica e tenta novamente em caso de 401."""
        params = {"page": page, "pageSize": page_size}
        if extra_params:
            params.update(extra_params)
        resp = client.get(
            f"{self.base_url}{path}", headers=self._headers, params=params
        )
        if resp.status_code == 401:
            self._reauth()
            resp = client.get(
                f"{self.base_url}{path}", headers=self._headers, params=params
            )
        resp.raise_for_status()
        return resp

    def get_paginated(
        self,
        path: str,
        page_size: int = PAGE_SIZE,
        extra_params: Optional[dict] = None,
        silent: bool = False,
    ) -> list:
        """Busca todas as páginas de um endpoint paginado.

        Args:
            silent: Se True, suprime logs de progresso por página (útil quando
                    chamado em loops para evitar flood de logs).
        """
        all_items: list = []
        page = 1
        total_pages: Optional[int] = None
        full_url = f"{self.base_url}{path}"
        while True:
            with self._client() as client:
                resp = self._get_page(client, path, page, page_size, extra_params)
                body = resp.json()
            items = body.get("data", [])
            if not items:
                break
            all_items.extend(items)
            if total_pages is None:
                total_pages = body.get("totalPages", 1)
            if not silent and total_pages > 1:
                params: dict = {"page": f"{page}/{total_pages}", "pageSize": page_size}
                if extra_params:
                    params.update(extra_params)
                params_str = ", ".join(f"{k}={v}" for k, v in params.items())
                log(f"GET {full_url} [{params_str}] → +{len(items)} itens")
            if page >= total_pages:
                break
            page += 1
        if not silent:
            pages_str = f"{total_pages} página(s)" if total_pages else "1 página"
            log(f"GET {full_url} — {len(all_items)} itens em {pages_str}")
        return all_items

    def get_single(self, path: str) -> dict:
        """GET simples sem paginação, retorna o corpo JSON da resposta."""
        with self._client() as client:
            resp = client.get(f"{self.base_url}{path}", headers=self._headers)
            if resp.status_code == 401:
                self._reauth()
                resp = client.get(f"{self.base_url}{path}", headers=self._headers)
            resp.raise_for_status()
            return resp.json()

    def get_raw(self, path: str) -> str:
        """GET simples sem paginação, retorna o corpo da resposta como texto bruto."""
        with self._client() as client:
            resp = client.get(f"{self.base_url}{path}", headers=self._headers)
            if resp.status_code == 401:
                self._reauth()
                resp = client.get(f"{self.base_url}{path}", headers=self._headers)
            resp.raise_for_status()
            return resp.text


@task
def get_authenticated_api_task() -> FMApi:
    """Cria e autentica o cliente da API Força Municipal."""
    use_proxy = env.API_FORCA_MUNICIPAL__USE_PROXY_URL
    proxy_url = env.API_FORCA_MUNICIPAL__PROXY_URL if use_proxy else None
    api = FMApi(base_url=env.API_FORCA_MUNICIPAL__API_URL, proxy_url=proxy_url)
    log(f"Conectando à API{' via proxy' if use_proxy else ''}: {api.base_url}")
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
    items = api.get_paginated(path=endpoint, page_size=page_size)
    if not items:
        log(f"Nenhum dado retornado de {endpoint}", level="warning")
        return pd.DataFrame()
    df = pd.DataFrame(items)
    log(f"DataFrame: {df.shape[0]} linhas × {df.shape[1]} colunas")
    return df


@task
def fetch_unit_positions_task(
    api: FMApi,
    data_inicio: Optional[str] = None,
    data_fim: Optional[str] = None,
    hora_inicio: str = "00:00:00",
    hora_fim: str = "23:59:59",
    page_size: int = PAGE_SIZE,
    endpoint: str = "/api/unit/positions",
    endpoint_unidades_ativas: str = "/api/unidades/ativas",
) -> pd.DataFrame:
    """
    Busca posições GPS de todas as unidades ativas para o intervalo especificado.

    Se data_inicio/data_fim não forem fornecidos, usa a data atual (America/São Paulo).
    Ideal para backfill: passe data_inicio e data_fim para um período histórico.
    """
    if data_inicio is None:
        data_inicio = datetime.now(tz=SP_TZ).strftime("%Y-%m-%d")
    if data_fim is None:
        data_fim = data_inicio

    log(f"Período: {data_inicio} {hora_inicio} → {data_fim} {hora_fim}")

    units = api.get_paginated(endpoint_unidades_ativas, page_size=page_size)
    unit_ids = [u["UnitId"] for u in units if u.get("UnitId")]
    log(f"{len(unit_ids)} unidades ativas encontradas")

    if not unit_ids:
        log("Nenhuma unidade ativa encontrada.", level="warning")
        return pd.DataFrame()

    all_positions: list = []
    total_units = len(unit_ids)
    for i, unit_id in enumerate(unit_ids, start=1):
        positions = api.get_paginated(
            endpoint,
            page_size=page_size,
            extra_params={
                "unitId": unit_id,
                "dataInicio": data_inicio,
                "dataFim": data_fim,
                "horaInicio": hora_inicio,
                "horaFim": hora_fim,
            },
            silent=True,
        )
        log(f"Unidade {i}/{total_units} ({unit_id}): {len(positions)} posições")
        all_positions.extend(positions)

    if not all_positions:
        log("Nenhuma posição encontrada para o período.", level="warning")
        return pd.DataFrame()

    df = pd.DataFrame(all_positions)
    log(f"Total: {df.shape[0]} posições de {total_units} unidades")
    return df


@task
def fetch_qmd_details_task(
    api: FMApi,
    page_size: int = PAGE_SIZE,
    endpoint: str = "/api/qmd",
    endpoint_qmd: str = "/api/qmd",
) -> pd.DataFrame:
    """
    Busca detalhes completos de todos os QMDs (missões, serviços, execuções).

    Faz primeiro GET /api/qmd para obter os IDs e depois GET /api/qmd/{id}
    para cada um. Campos aninhados (missoes, servicos, execucoes) são serializados
    como JSON string para compatibilidade com BigQuery.
    """
    qmds = api.get_paginated(endpoint_qmd, page_size=page_size)
    qmd_ids = [q["Id"] for q in qmds if q.get("Id")]
    log(f"{len(qmd_ids)} QMDs encontrados — buscando detalhes...")

    if not qmd_ids:
        log("Nenhum QMD encontrado.", level="warning")
        return pd.DataFrame()

    rows: list = []
    total = len(qmd_ids)
    for i, qmd_id in enumerate(qmd_ids, start=1):
        log(f"QMD {i}/{total} — GET {api.base_url}{endpoint}/{qmd_id}")
        body = api.get_single(f"{endpoint}/{qmd_id}")
        row = {
            k: json.dumps(v, ensure_ascii=False) if isinstance(v, (list, dict)) else v
            for k, v in body.items()
        }
        rows.append(row)

    df = pd.DataFrame(rows)
    log(f"DataFrame: {df.shape[0]} linhas × {df.shape[1]} colunas")
    return df


@task
def fetch_qmd_kml_task(
    api: FMApi,
    page_size: int = PAGE_SIZE,
    endpoint: str = "/api/qmd",
    endpoint_qmd: str = "/api/qmd",
) -> pd.DataFrame:
    """
    Busca arquivos KML de todos os QMDs e extrai todas as geometrias e metadados.

    Usa fastkml para parsear o KML sem perda de informação. Uma linha por
    Placemark com colunas fixas (qmd_id, kml_folder, name, description,
    geometry_wkt, geometry_type) e extended_data como JSON com todos os campos
    extras — o schema é estável mesmo que o KML evolua.

    Em BigQuery: ST_GEOGFROMTEXT(geometry_wkt) converte para GEOGRAPHY.
    """
    import io
    import warnings

    from fastkml import KML

    qmds = api.get_paginated(endpoint_qmd, page_size=page_size)
    qmd_ids = [q["Id"] for q in qmds if q.get("Id")]
    log(f"{len(qmd_ids)} QMDs encontrados — buscando KMLs...")

    if not qmd_ids:
        log("Nenhum QMD encontrado.", level="warning")
        return pd.DataFrame()

    rows: list = []
    total = len(qmd_ids)
    for i, qmd_id in enumerate(qmd_ids, start=1):
        log(f"KML {i}/{total} — GET {api.base_url}/api/qmd/{qmd_id}/kml")
        try:
            kml_content = api.get_raw(f"/api/qmd/{qmd_id}/kml")
        except Exception as exc:
            log(f"QMD {qmd_id}: KML não disponível ({exc})", level="warning")
            continue

        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                k = KML.parse(io.StringIO(kml_content), strict=False)
        except Exception as exc:
            log(f"QMD {qmd_id}: erro ao parsear KML ({exc})", level="warning")
            continue

        document = next(iter(k.features), None)
        if document is None:
            log(f"QMD {qmd_id}: KML sem Document.", level="warning")
            continue

        for folder in document.features:
            folder_name = getattr(folder, "name", None)
            for placemark in getattr(folder, "features", []):
                geom = getattr(placemark, "geometry", None)
                ed = getattr(placemark, "extended_data", None)
                ed_dict: dict = {}
                if ed is not None:
                    for elem in getattr(ed, "elements", []):
                        key = getattr(elem, "name", None)
                        if key is not None:
                            ed_dict[key] = getattr(elem, "value", None)

                rows.append(
                    {
                        "qmd_id": qmd_id,
                        "kml_folder": folder_name,
                        "name": getattr(placemark, "name", None),
                        "description": getattr(placemark, "description", None),
                        "geometry_wkt": geom.wkt if geom is not None else None,
                        "geometry_type": geom.geom_type if geom is not None else None,
                        "extended_data": (
                            json.dumps(ed_dict, ensure_ascii=False) if ed_dict else None
                        ),
                    }
                )

    if not rows:
        log("Nenhum KML retornado.", level="warning")
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    log(f"DataFrame: {df.shape[0]} linhas × {df.shape[1]} colunas")
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
    now = datetime.now(tz=SP_TZ).replace(tzinfo=None)
    df = df.copy()
    df["id_hash"] = content_hash
    df["updated_at"] = now

    savepath = f"/tmp/rj_segur__forca_municipal/{table_id}/{uuid.uuid4()}"
    Path(savepath).mkdir(parents=True, exist_ok=True)
    df, partition_cols = parse_date_columns(df, "updated_at")

    cols_to_str = [c for c in df.columns]
    df[cols_to_str] = df[cols_to_str].astype(str)

    to_partitions(
        data=df,
        partition_columns=partition_cols,
        savepath=savepath,
        data_type="parquet",
        suffix=content_hash,
    )
    log(f"Partições salvas: {savepath} (hash={content_hash})")
    return savepath
