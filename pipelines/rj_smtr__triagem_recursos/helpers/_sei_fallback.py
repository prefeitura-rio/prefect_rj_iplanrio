"""Cliente HTTP síncrono fallback para `mod-wssei` v2 quando `sei-client-smtr` não está instalado.

Permite manter o adaptador `helpers/sei_client.py` pronto para `sei-client-smtr`,
ao mesmo tempo em que garante build/execução no cluster enquanto o repositório
externo privado não estiver acessível no Docker build.
"""

from __future__ import annotations

import base64
import html
import json
import random
import re
import threading
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from http import HTTPStatus
from typing import Any

import httpx

from prefect_rj_iplanrio.logging import get_logger

logger = get_logger(__name__)

CHUNK_SIZE = 64 * 1024
STATUS_RETENTAVEIS = frozenset({HTTPStatus.BAD_GATEWAY, HTTPStatus.SERVICE_UNAVAILABLE, HTTPStatus.GATEWAY_TIMEOUT})
_RE_ATRIBUIDO = re.compile(r"\(atribuíd[oa] a (.+?)\)\s*$")


@dataclass(frozen=True)
class FallbackUsuarioSessao:
    """Sessão mínima retornada por `/autenticar`."""

    id_usuario: str
    sigla: str | None = None
    nome: str | None = None
    unidade_sessao: str | None = None
    token: str = field(default="", repr=False)


@dataclass(frozen=True)
class FallbackAtribuidoA:
    """Usuário responsável resolvido a partir de `dadosAbertura` ou `usuarioAtribuido`."""

    id_usuario: str | None
    nome: str
    sigla: str | None = None


@dataclass(frozen=True)
class FallbackMarcadorSEI:
    """Marcador ativo no processo."""

    nome: str


@dataclass(frozen=True)
class FallbackProcessoSEI:
    """Representação normalizada compatível com `sei_client_smtr.ProcessoSEI`."""

    id_procedimento: str
    protocolo: str
    atribuido_unidade_atual: FallbackAtribuidoA | None = None
    unidade_geradora_sigla: str | None = None
    data_autuacao: str | None = None
    marcador: FallbackMarcadorSEI | None = None
    raw: dict[str, Any] = field(default_factory=dict, repr=False)


@dataclass(frozen=True)
class FallbackDocumentoSEI:
    """Metadados de documento compatíveis com `sei_client_smtr.DocumentoSEI`."""

    id: str
    id_procedimento: str = ""
    protocolo_formatado: str = ""
    tipo: str = ""
    tipo_documento: str = ""
    mime_type: str = ""
    nome_composto: str = ""
    cancelado: bool = False
    raw: dict[str, Any] = field(default_factory=dict, repr=False)


@dataclass(frozen=True)
class FallbackConteudoDocumento:
    """Bytes de anexo baixados em RAM compatíveis com `sei_client_smtr.ConteudoDocumento`."""

    conteudo: bytes = field(repr=False)
    content_type: str = ""

    @property
    def is_pdf(self) -> bool:
        """True quando os bytes iniciam com `%PDF`."""
        return self.conteudo[:4] == b"%PDF"


@dataclass(frozen=True)
class FallbackPaginaResultado:
    """Envelope paginado compatível com `sei_client_smtr.PaginaResultado`."""

    itens: list[Any]
    pagina: int
    tamanho_pagina: int
    total_itens: int
    tem_proxima: bool


@dataclass(frozen=True)
class FallbackHooks:
    """Callbacks leves compatíveis com `sei_client_smtr.SeiHooks`."""

    on_retry: Callable[[str, str, int, int | None, float], None] | None = None
    on_response: Callable[[str, str, int, float], None] | None = None


def _unescape(val: Any) -> str:
    return html.unescape(str(val or "")).strip()


def _sn(val: Any) -> bool:
    return val is True or str(val or "").strip().upper() == "S"


def _resolver_atribuido(att: dict[str, Any], unidade_id: str | None) -> FallbackAtribuidoA | None:
    if not unidade_id:
        return None
    uid = str(unidade_id)
    da = att.get("dadosAbertura") if isinstance(att.get("dadosAbertura"), dict) else {}
    unidades = da.get("unidades") if isinstance(da.get("unidades"), list) else []
    lista = da.get("lista") if isinstance(da.get("lista"), list) else []
    ua = att.get("usuarioAtribuido") if isinstance(att.get("usuarioAtribuido"), dict) else {}
    unidade_topo = att.get("unidade") if isinstance(att.get("unidade"), dict) else {}

    for idx, unid in enumerate(unidades):
        if isinstance(unid, dict) and str(unid.get("id")) == uid:
            sigla_item = str(lista[idx].get("sigla") or "") if idx < len(lista) and isinstance(lista[idx], dict) else ""
            match = _RE_ATRIBUIDO.search(sigla_item)
            if match:
                return FallbackAtribuidoA(id_usuario=None, nome=_unescape(match.group(1)))
            break

    sigla_ua = _unescape(ua.get("sigla")) or None
    id_ua = str(ua["idUsuario"]) if ua.get("idUsuario") not in (None, "") else None
    if ua and (str(unidade_topo.get("idUnidade")) == uid or (len(unidades) == 1 and str(unidades[0].get("id")) == uid)):
        return FallbackAtribuidoA(id_usuario=id_ua, nome=_unescape(ua.get("nome")), sigla=sigla_ua)
    return None


def _parse_processo(raw: dict[str, Any], unidade_id: str | None) -> FallbackProcessoSEI:
    att = raw.get("atributos") if isinstance(raw.get("atributos"), dict) else {}
    unidade_topo = att.get("unidade") if isinstance(att.get("unidade"), dict) else {}
    marc = att.get("marcador")
    marcador = None
    if isinstance(marc, list) and marc and isinstance(marc[0], dict):
        nome_m = _unescape(marc[0].get("nome"))
        if nome_m:
            marcador = FallbackMarcadorSEI(nome=nome_m)

    return FallbackProcessoSEI(
        id_procedimento=str(att.get("idProcedimento") or raw.get("idProcedimento") or raw.get("id") or ""),
        protocolo=_unescape(att.get("numero") or att.get("protocoloFormatado") or raw.get("protocoloFormatado") or raw.get("numero")),
        atribuido_unidade_atual=_resolver_atribuido(att, unidade_id),
        unidade_geradora_sigla=_unescape(unidade_topo.get("sigla") or raw.get("siglaUnidade")) or None,
        data_autuacao=_unescape(raw.get("dataGeracao") or att.get("dataGeracao")) or None,
        marcador=marcador,
        raw=dict(raw),
    )


def _parse_documento(raw: dict[str, Any], id_procedimento: str) -> FallbackDocumentoSEI:
    attrs = raw.get("atributos") if isinstance(raw.get("atributos"), dict) else {}
    status = attrs.get("status") if isinstance(attrs.get("status"), dict) else {}
    return FallbackDocumentoSEI(
        id=str(raw.get("id") or raw.get("idProtocolo") or attrs.get("idProtocolo") or ""),
        id_procedimento=str(attrs.get("idProcedimento") or id_procedimento or ""),
        protocolo_formatado=_unescape(attrs.get("protocoloFormatado") or raw.get("protocoloFormatado") or raw.get("documentoFormatado")),
        tipo=_unescape(attrs.get("tipo")),
        tipo_documento=_unescape(attrs.get("tipoDocumento")),
        mime_type=_unescape(attrs.get("mimeType")),
        nome_composto=_unescape(attrs.get("nomeComposto")),
        cancelado=_sn(status.get("documentoCancelado")),
        raw=dict(raw),
    )


def _extrair_lista_e_total(payload: dict[str, Any], chave: str) -> tuple[list[dict[str, Any]], int]:
    raw_data = payload.get("data")
    if isinstance(raw_data, dict):
        itens = raw_data.get(chave) or []
        total_raw = payload.get("total") or payload.get("total_itens") or raw_data.get("total") or raw_data.get("total_itens")
    elif isinstance(raw_data, list):
        itens = raw_data
        total_raw = payload.get("total") or payload.get("total_itens")
    else:
        itens = []
        total_raw = 0
    validos = [x for x in itens if isinstance(x, dict)]
    try:
        total = int(total_raw) if total_raw not in (None, "") else len(validos)
    except (TypeError, ValueError):
        total = len(validos)
    return validos, total


class FallbackSeiSync:
    """Implementação síncrona autônoma do `mod-wssei` v2 compatível com `SeiClientSync`."""

    def __init__(
        self,
        *,
        url: str,
        usuario: str,
        senha: str,
        orgao: str = "12",
        unidade_id: str | None = None,
        timeout: float = 60.0,
        verify_ssl: bool = True,
        max_connections: int = 16,
        max_download_bytes: int = 30 * 1024 * 1024,
        http_client: httpx.Client | None = None,
        hooks: FallbackHooks | None = None,
    ) -> None:
        self.base_url = url.strip().rstrip("/")
        self.usuario = usuario.strip()
        self.senha = senha
        self.orgao = str(orgao).strip() or "12"
        self.unidade_id = str(unidade_id).strip() if unidade_id else None
        self.timeout_s = float(timeout)
        self.verify_ssl = verify_ssl
        self.max_connections = max(4, max_connections)
        self.max_download_bytes = max_download_bytes
        self._hooks = hooks or FallbackHooks()

        self._owns_http_client = http_client is None
        self._http_client = http_client
        self._client_lock = threading.Lock()
        self._auth_lock = threading.Lock()
        self._token: str | None = None
        self._sessao: FallbackUsuarioSessao | None = None

    @property
    def token(self) -> str | None:
        """Token JWT ativo."""
        return self._token

    @property
    def sessao(self) -> FallbackUsuarioSessao | None:
        """Dados da última sessão autenticada."""
        return self._sessao

    @property
    def http_client(self) -> httpx.Client:
        """Pool `httpx.Client` compartilhado."""
        with self._client_lock:
            if self._http_client is None:
                self._http_client = httpx.Client(
                    limits=httpx.Limits(
                        max_connections=self.max_connections,
                        max_keepalive_connections=max(4, min(self.max_connections, 12)),
                        keepalive_expiry=30.0,
                    ),
                    timeout=self._build_timeout(),
                    verify=self.verify_ssl,
                )
            return self._http_client

    def _build_timeout(self, override: float | None = None) -> httpx.Timeout:
        read_s = float(override) if override is not None else self.timeout_s
        return httpx.Timeout(connect=5.0, read=read_s, write=min(read_s, 30.0), pool=10.0)

    def close(self) -> None:
        """Fecha o pool HTTP se pertencer a esta instância."""
        with self._client_lock:
            if self._http_client is not None and self._owns_http_client:
                self._http_client.close()
                self._http_client = None

    def _headers(self) -> tuple[str, dict[str, str]]:
        if not self._token:
            with self._auth_lock:
                if not self._token:
                    self.autenticar()
        tok = str(self._token)
        headers = {"token": tok}
        if self.unidade_id:
            headers["unidade"] = self.unidade_id
        return tok, headers

    def _reautenticar_se_necessario(self, token_usado: str | None) -> None:
        with self._auth_lock:
            if self._token and self._sessao is not None and self._token != token_usado:
                return
            self.autenticar()

    def _backoff(self, attempt: int, retry_after: str | None = None) -> float:
        if retry_after:
            try:
                return min(max(0.0, float(retry_after.strip())), 60.0)
            except ValueError:
                pass
        return float(random.uniform(0.0, min(1.0 * (2.0**attempt), 8.0)))

    def autenticar(
        self,
        usuario: str | None = None,
        senha: str | None = None,
        orgao: str | int | None = None,
    ) -> FallbackUsuarioSessao:
        """Autentica em `POST /autenticar`."""
        user = (usuario if usuario is not None else self.usuario).strip()
        pwd = senha if senha is not None else self.senha
        org = self.orgao if orgao is None else str(orgao)
        url = f"{self.base_url}/autenticar"
        resp = self.http_client.post(url, data={"usuario": user, "senha": pwd, "orgao": org}, timeout=self._build_timeout())
        if resp.status_code != HTTPStatus.OK:
            raise RuntimeError(f"Falha na autenticacao do SEI ({resp.status_code}): {resp.text}")
        data = resp.json()
        if not data.get("sucesso"):
            raise RuntimeError(f"O SEI recusou a autenticacao: {data.get('mensagem', data)}")
        payload_data = data.get("data") or {}
        tok = str(payload_data.get("token") or "").strip()
        if not tok:
            raise RuntimeError("Resposta do SEI nao continha o campo 'token'.")
        login = payload_data.get("loginData") or {}
        sessao = FallbackUsuarioSessao(
            id_usuario=str(login.get("IdUsuario") or login.get("idUsuario") or ""),
            sigla=_unescape(login.get("sigla")) or None,
            nome=_unescape(login.get("nome")) or None,
            unidade_sessao=str(login.get("IdUnidadeAtual") or "") or None,
            token=tok,
        )
        self._token = tok
        self._sessao = sessao
        logger.info("SEI (fallback) autenticado com sucesso (sigla=%s)", sessao.sigla or user)
        return sessao

    def _executar_http_com_reauth(
        self,
        method: str,
        endpoint: str,
        *,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        json_data: dict[str, Any] | None = None,
        timeout: float | None = None,
        operacao: str = "",
    ) -> httpx.Response:
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        req_timeout = self._build_timeout(timeout)
        max_attempts = 5

        for attempt in range(max_attempts):
            tok_usado, headers = self._headers()
            t0 = time.monotonic()
            try:
                resp = self.http_client.request(
                    method=method,
                    url=url,
                    headers=headers,
                    params=params,
                    data=data,
                    json=json_data,
                    timeout=req_timeout,
                )
                if self._hooks.on_response is not None:
                    self._hooks.on_response(method, endpoint, resp.status_code, (time.monotonic() - t0) * 1000.0)
                if resp.status_code in (HTTPStatus.UNAUTHORIZED, HTTPStatus.FORBIDDEN):
                    logger.warning("Token expirado (HTTP %d) em %s. Reautenticando...", resp.status_code, endpoint)
                    self._reautenticar_se_necessario(tok_usado)
                    _, headers = self._headers()
                    resp = self.http_client.request(
                        method=method,
                        url=url,
                        headers=headers,
                        params=params,
                        data=data,
                        json=json_data,
                        timeout=req_timeout,
                    )
            except httpx.TransportError as exc:
                if attempt + 1 < max_attempts:
                    atraso = self._backoff(attempt)
                    if self._hooks.on_retry is not None:
                        self._hooks.on_retry(method, endpoint, attempt + 1, None, atraso)
                    time.sleep(atraso)
                    continue
                raise RuntimeError(f"Erro de transporte HTTP no SEI ({operacao}): {exc}") from exc

            if resp.status_code in STATUS_RETENTAVEIS and attempt + 1 < max_attempts:
                atraso = self._backoff(attempt, resp.headers.get("Retry-After"))
                if self._hooks.on_retry is not None:
                    self._hooks.on_retry(method, endpoint, attempt + 1, resp.status_code, atraso)
                time.sleep(atraso)
                continue
            return resp

        raise RuntimeError(f"SEI indisponivel em {operacao} apos retentativas.")

    def _request_json(self, method: str, endpoint: str, *, params: dict[str, Any] | None = None) -> dict[str, Any]:
        resp = self._executar_http_com_reauth(method, endpoint, params=params, operacao=f"{method} {endpoint}")
        if resp.status_code != HTTPStatus.OK:
            raise RuntimeError(f"Falha em {endpoint} ({resp.status_code}): {resp.text}")
        payload = resp.json()
        if not isinstance(payload, dict) or not payload.get("sucesso"):
            msg = payload.get("mensagem", payload) if isinstance(payload, dict) else payload
            raise RuntimeError(f"Erro retornado pelo SEI em {endpoint}: {msg}")
        return payload

    def listar_processos_pagina(
        self,
        *,
        limit: int | None = None,
        start: int = 0,
        apenas_meus: bool = False,
        usuario: str | int | None = None,
    ) -> FallbackPaginaResultado:
        """Busca uma página (0-indexed) de `GET /processo/listar`."""
        tam = max(1, limit if limit is not None else 100)
        params: dict[str, Any] = {"limit": tam, "start": start}
        if apenas_meus:
            params["apenasMeus"] = "S"
        if usuario not in (None, ""):
            params["usuario"] = str(usuario)
        payload = self._request_json("GET", "/processo/listar", params=params)
        brutos, total_itens = _extrair_lista_e_total(payload, "processos")
        itens = [_parse_processo(item, self.unidade_id) for item in brutos]
        tem_proxima = len(brutos) > 0 and ((start + 1) * tam < total_itens if total_itens > 0 else len(brutos) >= tam)
        return FallbackPaginaResultado(itens=itens, pagina=start, tamanho_pagina=tam, total_itens=total_itens, tem_proxima=tem_proxima)

    def iterar_paginas_processos(
        self,
        *,
        limit: int | None = None,
        start: int = 0,
        max_paginas: int = 500,
        apenas_meus: bool = False,
        usuario: str | int | None = None,
    ) -> Iterator[FallbackPaginaResultado]:
        """Itera páginas deduplicadas de `GET /processo/listar`."""
        tam = max(1, limit if limit is not None else 100)
        vistos: set[str] = set()
        for pagina_idx in range(start, start + max_paginas):
            pg = self.listar_processos_pagina(limit=tam, start=pagina_idx, apenas_meus=apenas_meus, usuario=usuario)
            if not pg.itens:
                break
            novos: list[FallbackProcessoSEI] = []
            for proc in pg.itens:
                chave = proc.id_procedimento or proc.protocolo
                if chave and chave in vistos:
                    continue
                if chave:
                    vistos.add(chave)
                novos.append(proc)
            if not novos:
                break
            esgotou = (pg.total_itens > 0 and len(vistos) >= pg.total_itens) or (pg.total_itens == 0 and len(pg.itens) < tam)
            yield FallbackPaginaResultado(itens=novos, pagina=pagina_idx, tamanho_pagina=tam, total_itens=pg.total_itens, tem_proxima=not esgotou)
            if esgotou:
                break

    def consultar_processo_por_id(self, id_procedimento: str | int) -> FallbackProcessoSEI:
        """Consulta processo por `IdProcedimento` (`GET /processo/consultar/{id}`)."""
        id_str = str(id_procedimento).strip()
        dados = self._request_json("GET", f"/processo/consultar/{id_str}").get("data") or {}
        return FallbackProcessoSEI(id_procedimento=id_str, protocolo=_unescape(dados.get("ProtocoloProcedimentoFormatado") or dados.get("protocoloFormatado")), raw=dict(dados))

    def consultar_processo_completo(self, protocolo_formatado: str) -> FallbackProcessoSEI:
        """Consulta processo por número formatado em 2 etapas."""
        d1 = self._request_json("GET", "/processo/consultar", params={"protocoloFormatado": protocolo_formatado.strip()}).get("data") or {}
        id_proc = str(d1.get("IdProcedimento") or "")
        if not id_proc:
            raise RuntimeError(f"Processo {protocolo_formatado} nao encontrado no SEI.")
        try:
            d2 = self._request_json("GET", f"/processo/consultar/{id_proc}").get("data") or {}
        except Exception:
            d2 = {}
        comb = {**d1, **(d2 if isinstance(d2, dict) else {})}
        return FallbackProcessoSEI(id_procedimento=id_proc, protocolo=_unescape(comb.get("ProtocoloProcedimentoFormatado") or protocolo_formatado), raw=comb)

    def listar_documentos_todos(self, id_procedimento: str | int, *, max_paginas: int = 25, page_size: int = 200) -> FallbackPaginaResultado:
        """Lista todos os documentos paginando `GET /documento/listar/{id}` (evita corte em 10)."""
        proc_str = str(id_procedimento).strip()
        docs: list[FallbackDocumentoSEI] = []
        total = 0
        for pagina in range(max_paginas):
            payload = self._request_json("GET", f"/documento/listar/{proc_str}", params={"limit": page_size, "start": pagina})
            brutos, total_pg = _extrair_lista_e_total(payload, "documentos")
            if pagina == 0 or total_pg:
                total = total_pg
            if not brutos:
                break
            docs.extend(_parse_documento(item, proc_str) for item in brutos)
            if len(docs) >= total:
                break
        return FallbackPaginaResultado(itens=docs, pagina=0, tamanho_pagina=page_size, total_itens=total, tem_proxima=len(docs) < total)

    def _stream_anexo_unico(
        self,
        url: str,
        id_documento: str,
        headers: dict[str, str],
        max_bytes: int,
        req_timeout: httpx.Timeout,
    ) -> tuple[int, dict[str, str], bytes | None]:
        chunks: list[bytes] = []
        total_bytes = 0
        with self.http_client.stream("GET", url, headers=headers, timeout=req_timeout) as resp:
            resp_headers = dict(resp.headers)
            if resp.status_code in (HTTPStatus.UNAUTHORIZED, HTTPStatus.FORBIDDEN) or resp.status_code in STATUS_RETENTAVEIS:
                return resp.status_code, resp_headers, None
            if resp.status_code != HTTPStatus.OK:
                raise RuntimeError(f"Falha ao baixar anexo {id_documento} no mod-wssei ({resp.status_code})")
            primeiro = True
            for chunk in resp.iter_bytes(chunk_size=CHUNK_SIZE):
                if not chunk:
                    continue
                if primeiro:
                    primeiro = False
                    if b'"sucesso":false' in chunk or (b"{" in chunk and b'"mensagem"' in chunk):
                        raise RuntimeError(f"O SEI retornou erro ao baixar anexo {id_documento}: {chunk.decode('utf-8', errors='ignore')}")
                total_bytes += len(chunk)
                if total_bytes > max_bytes:
                    raise ValueError(f"Documento {id_documento} excede o limite maximo permitido ({total_bytes} > {max_bytes} bytes).")
                chunks.append(chunk)
        return HTTPStatus.OK, resp_headers, b"".join(chunks)

    def baixar_documento(
        self,
        id_documento: str | int,
        *,
        max_bytes: int | None = None,
        timeout: float | None = None,
    ) -> FallbackConteudoDocumento:
        """Baixa anexo em streaming chunked com retry e decodificação binária/JSON."""
        doc_str = str(id_documento).strip()
        endpoint = f"/documento/baixar/anexo/{doc_str}"
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        limite = max_bytes if max_bytes is not None else self.max_download_bytes
        req_timeout = self._build_timeout(timeout)
        max_attempts = 5

        for attempt in range(max_attempts):
            tok_usado, headers = self._headers()
            t0 = time.monotonic()
            try:
                status, resp_headers, raw = self._stream_anexo_unico(url, doc_str, headers, limite, req_timeout)
                if self._hooks.on_response is not None:
                    self._hooks.on_response("GET", endpoint, status, (time.monotonic() - t0) * 1000.0)
                if status in (HTTPStatus.UNAUTHORIZED, HTTPStatus.FORBIDDEN):
                    self._reautenticar_se_necessario(tok_usado)
                    _, headers = self._headers()
                    status, resp_headers, raw = self._stream_anexo_unico(url, doc_str, headers, limite, req_timeout)
            except httpx.TransportError as exc:
                if attempt + 1 < max_attempts:
                    atraso = self._backoff(attempt)
                    if self._hooks.on_retry is not None:
                        self._hooks.on_retry("GET", endpoint, attempt + 1, None, atraso)
                    time.sleep(atraso)
                    continue
                raise RuntimeError(f"Erro de transporte HTTP ao baixar anexo {doc_str}: {exc}") from exc

            if status in STATUS_RETENTAVEIS and attempt + 1 < max_attempts:
                atraso = self._backoff(attempt, resp_headers.get("Retry-After"))
                if self._hooks.on_retry is not None:
                    self._hooks.on_retry("GET", endpoint, attempt + 1, status, atraso)
                time.sleep(atraso)
                continue
            if raw is None:
                raise RuntimeError(f"Falha ao baixar anexo {doc_str} no mod-wssei ({status})")

            ctype = resp_headers.get("content-type", "")
            if "json" in ctype.lower() or raw.lstrip().startswith(b"{"):
                try:
                    payload = json.loads(raw.decode("utf-8", errors="replace"))
                    if isinstance(payload, dict) and "sucesso" in payload:
                        if not payload.get("sucesso"):
                            raise RuntimeError(f"Erro ao baixar anexo {doc_str}: {payload.get('mensagem')}")
                        df = payload.get("data")
                        if isinstance(df, str):
                            return FallbackConteudoDocumento(conteudo=base64.b64decode(df), content_type="application/pdf")
                        if isinstance(df, dict) and "html" in df:
                            return FallbackConteudoDocumento(conteudo=str(df["html"] or "").encode("utf-8"), content_type="text/html")
                except ValueError:
                    pass
            return FallbackConteudoDocumento(conteudo=raw, content_type=ctype)

        raise RuntimeError(f"Falha ao baixar anexo {doc_str} apos esgotar retentativas.")
