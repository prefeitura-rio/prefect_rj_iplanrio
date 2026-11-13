"""HTTP client for SEI-Rio integration via the mod-wssei REST API.

Provides streaming downloads with strict memory limits, token auto-renewal,
retries with backoff on 502/503/504, and process listing for JARI triage.
"""

import threading
import time
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from http import HTTPStatus
from typing import Any, Self

import httpx
import pymupdf as fitz

from pipelines.rj_smtr__triagem_recursos.constants import obter_env
from pipelines.rj_smtr__triagem_recursos.helpers.windowing import FITZ_LOCK
from prefect_rj_iplanrio.logging import get_logger

logger = get_logger(__name__)

MAX_DOCUMENT_BYTES = 30 * 1024 * 1024  # 30 MB teto de memoria por documento
CHUNK_SIZE = 64 * 1024  # 64 KB por chunk de streaming
STATUS_RETENTAVEIS = frozenset({HTTPStatus.BAD_GATEWAY, HTTPStatus.SERVICE_UNAVAILABLE, HTTPStatus.GATEWAY_TIMEOUT})
ATRASOS_RETENTATIVA = (0.5, 1.0, 2.0)


@dataclass(frozen=True)
class SeiConfig:
    """Connection and authentication configuration for mod-wssei REST API."""

    base_url: str = "http://localhost:8000"
    usuario: str = "usuario_servico"
    senha: str = "senha_servico"
    id_unidade: str = "110000001"
    orgao: int = 12
    timeout: int = 60
    verify_ssl: bool = True

    @classmethod
    def from_env(cls, is_local: bool = False) -> "SeiConfig":
        """Carrega configuração SEI das variáveis de ambiente, exigindo credenciais em cloud/staging."""
        env_base_url = obter_env("SEI_API_BASE_URL") or ""
        env_usuario = obter_env("SEI_SERVICE_USER") or ""
        env_senha = obter_env("SEI_SERVICE_PASSWORD") or ""
        env_unidade = obter_env("SEI_ID_UNIDADE") or ""
        env_verify_raw = (obter_env("SEI_VERIFY_SSL", "true") or "true").lower()
        env_verify_ssl = env_verify_raw not in ("false", "0", "no", "disabled")

        if not is_local:
            ausentes = [
                nome
                for nome, val in (
                    ("SEI_API_BASE_URL", env_base_url),
                    ("SEI_SERVICE_USER", env_usuario),
                    ("SEI_SERVICE_PASSWORD", env_senha),
                    ("SEI_ID_UNIDADE", env_unidade),
                )
                if not val
            ]
            if ausentes:
                raise RuntimeError(
                    f"Variáveis obrigatórias do SEI ausentes no ambiente staging/cloud: {', '.join(ausentes)}"
                )

        return cls(
            base_url=env_base_url or "http://localhost:8000",
            usuario=env_usuario or "usuario_servico",
            senha=env_senha or "senha_servico",
            id_unidade=env_unidade or "110000001",
            orgao=int(obter_env("SEI_ORGAO", "12") or "12"),
            timeout=int(obter_env("SEI_TIMEOUT", "60") or "60"),
            verify_ssl=env_verify_ssl,
        )


class ModWsseiClient:
    """HTTP client communicating with the SEI-Rio REST API (mod-wssei)."""

    def __init__(
        self,
        config: SeiConfig | None = None,
        base_url: str | None = None,
        is_local: bool = False,
        http_client: httpx.Client | None = None,
        max_connections: int = 16,
    ) -> None:
        """Initialize the SEI client with config or custom base URL."""
        if config is not None:
            self.config = config
        elif base_url is not None:
            cfg_env = SeiConfig.from_env(is_local=bool(is_local or obter_env("SEI_SERVICE_USER") is None))
            self.config = SeiConfig(
                base_url=base_url,
                usuario=cfg_env.usuario,
                senha=cfg_env.senha,
                id_unidade=cfg_env.id_unidade,
                orgao=cfg_env.orgao,
                timeout=cfg_env.timeout,
                verify_ssl=cfg_env.verify_ssl,
            )
        else:
            self.config = SeiConfig.from_env(is_local=is_local)

        self.base_url = self.config.base_url.rstrip("/")
        self.token: str | None = None
        self.sigla_usuario: str | None = None
        self.nome_usuario: str | None = None
        self._auth_lock = threading.Lock()
        self._client_lock = threading.Lock()
        self._max_connections = max(4, max_connections)
        self._owns_http_client = http_client is None
        self._http_client: httpx.Client | None = http_client

    @property
    def http_client(self) -> httpx.Client:
        """Expõe o pool HTTP persistente compartilhado pelo cliente."""
        return self.get_http_client()

    def get_http_client(self) -> httpx.Client:
        """Retorna o pool httpx.Client compartilhado e thread-safe."""
        with self._client_lock:
            if self._http_client is None:
                keepalive = max(4, min(self._max_connections, 12))
                self._http_client = httpx.Client(
                    limits=httpx.Limits(
                        max_connections=self._max_connections,
                        max_keepalive_connections=keepalive,
                        keepalive_expiry=30.0,
                    ),
                    timeout=httpx.Timeout(float(self.config.timeout), connect=5.0),
                    verify=self.config.verify_ssl,
                )
            return self._http_client

    def close(self) -> None:
        """Fecha o pool HTTP compartilhado quando pertencente a esta instância."""
        with self._client_lock:
            if self._http_client is not None and self._owns_http_client:
                self._http_client.close()
                self._http_client = None

    def __enter__(self) -> Self:
        """Suporta uso com gerenciador de contexto."""
        return self

    def __exit__(self, *args: object) -> None:
        """Garante liberação do pool HTTP ao sair do contexto."""
        self.close()

    def reautenticar_se_necessario(self, token_usado: str | None) -> str:
        """Renova JWT sob lock para evitar tempestade de logins paralelos."""
        with self._auth_lock:
            if self.token and self.token != token_usado:
                return self.token
            return self.autenticar()

    def autenticar(
        self,
        usuario: str | None = None,
        senha: str | None = None,
        orgao: int | None = None,
    ) -> str:
        """Authenticate via POST /autenticar and store JWT token."""
        user = usuario or self.config.usuario
        pwd = senha or self.config.senha
        org = self.config.orgao if orgao is None else orgao

        url = f"{self.base_url}/autenticar"
        payload = {
            "usuario": user,
            "senha": pwd,
            "orgao": org,
        }

        logger.info("Autenticando no mod-wssei em: %s (usuario: %s)", url, user)
        client = self.get_http_client()
        response = client.post(url, data=payload, timeout=httpx.Timeout(float(self.config.timeout), connect=5.0))

        if response.status_code != HTTPStatus.OK:
            raise RuntimeError(f"Falha na autenticacao do SEI ({response.status_code}): {response.text}")

        data = response.json()
        if not data.get("sucesso"):
            raise RuntimeError(f"O SEI recusou a autenticacao: {data.get('mensagem', data)}")

        payload_data = data.get("data", {})
        token = payload_data.get("token")
        if not token:
            raise RuntimeError("Resposta do SEI nao continha o campo 'token'.")

        login_data = payload_data.get("loginData") or {}
        self.sigla_usuario = (login_data.get("sigla") or "").strip() or None
        self.nome_usuario = (login_data.get("nome") or "").strip() or None

        self.token = token
        logger.info("Autenticacao no mod-wssei concluida com sucesso (sigla: %s).", self.sigla_usuario)
        return token

    def obter_headers(self) -> dict[str, str]:
        """Return default headers with auth token and active JARI unit."""
        if not self.token:
            with self._auth_lock:
                if not self.token:
                    self.autenticar()

        headers = {"token": str(self.token)}
        if self.config.id_unidade:
            headers["unidade"] = str(self.config.id_unidade)
        return headers

    def executar_requisicao(
        self,
        metodo: str,
        endpoint: str,
        params: dict[str, Any] | None = None,
        json_data: dict[str, Any] | None = None,
        timeout: int | float | None = None,
    ) -> httpx.Response:
        """Execute HTTP request with retry on 502/503/504 and re-auth on 401/403."""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        req_timeout = httpx.Timeout(float(timeout or self.config.timeout), connect=5.0)
        client = self.get_http_client()

        for tentativa, atraso in enumerate((*ATRASOS_RETENTATIVA, None)):
            headers = self.obter_headers()
            try:
                response = client.request(
                    method=metodo,
                    url=url,
                    headers=headers,
                    params=params,
                    json=json_data,
                    timeout=req_timeout,
                )

                if response.status_code in (HTTPStatus.UNAUTHORIZED, HTTPStatus.FORBIDDEN):
                    logger.warning("Token expirado (HTTP %d). Reautenticando...", response.status_code)
                    self.reautenticar_se_necessario(headers.get("token"))
                    headers = self.obter_headers()
                    response = client.request(
                        method=metodo,
                        url=url,
                        headers=headers,
                        params=params,
                        json=json_data,
                        timeout=req_timeout,
                    )

                if response.status_code not in STATUS_RETENTAVEIS or atraso is None:
                    return response

                logger.warning(
                    "SEI respondeu HTTP %d; retentativa %d/%d em %.1fs",
                    response.status_code,
                    tentativa + 1,
                    len(ATRASOS_RETENTATIVA),
                    atraso,
                )
                time.sleep(atraso)
            except httpx.TransportError as exc:
                if atraso is None:
                    raise RuntimeError(f"Erro de transporte HTTP no SEI apos retentativas: {exc}") from exc
                logger.warning("Erro de conexao no SEI (%s); retentando em %.1fs", exc, atraso)
                time.sleep(atraso)

        raise RuntimeError("Inalcancavel")

    def _buscar_pagina_caixa(
        self,
        apenas_meus: bool = False,
        usuario_id: int | str | None = None,
        limit: int = 100,
        start: int = 0,
    ) -> tuple[list[dict[str, Any]], int]:
        """Fetch a single inbox page from mod-wssei and return (processes, reported_total)."""
        endpoint = "/processo/listar"
        params: dict[str, Any] = {
            "limit": limit,
            "start": start,
        }
        if apenas_meus:
            params["apenasMeus"] = "S"
        if usuario_id is not None:
            params["idUsuarioAtribuido"] = str(usuario_id)

        response = self.executar_requisicao("GET", endpoint, params=params)
        if response.status_code != HTTPStatus.OK:
            raise RuntimeError(f"Falha ao listar processos ({response.status_code}): {response.text}")

        data = response.json()
        if not data.get("sucesso"):
            raise RuntimeError(f"Erro retornado pelo SEI ao listar processos: {data.get('mensagem')}")

        raw_data = data.get("data")
        if isinstance(raw_data, dict):
            processos_brutos = raw_data.get("processos", []) or []
            total_raw = (
                data.get("total")
                or data.get("total_itens")
                or raw_data.get("total")
                or raw_data.get("total_itens")
            )
        elif isinstance(raw_data, list):
            processos_brutos = raw_data
            total_raw = data.get("total") or data.get("total_itens")
        else:
            processos_brutos = []
            total_raw = 0

        try:
            total_itens = int(total_raw or 0)
        except (TypeError, ValueError):
            total_itens = 0

        lista_processos: list[dict[str, Any]] = []
        for p in processos_brutos:
            atributos = p.get("atributos") if isinstance(p.get("atributos"), dict) else {}
            id_proc = p.get("idProcedimento") or p.get("id") or atributos.get("idProcedimento")
            proto_fmt = str(p.get("protocoloFormatado") or atributos.get("protocoloFormatado") or "").strip()
            usuario_atrib = p.get("usuarioAtribuido") or atributos.get("usuarioAtribuido")
            id_doc = p.get("idDocumento") or atributos.get("idDocumento")

            proc_formatado = {
                "processo_id": id_proc,
                "id_procedimento_sei": str(id_proc or ""),
                "numero_processo_sei": proto_fmt,
                "numero_processo": proto_fmt,
                "id_documento_sei": id_doc,
                "id_documento": id_doc,
                "sigla_unidade": p.get("siglaUnidade") or atributos.get("siglaUnidade") or "10_JARI",
                "usuario_atribuido_sigla": usuario_atrib.get("sigla")
                if isinstance(usuario_atrib, dict)
                else None,
                "data_autuacao": p.get("dataGeracao") or atributos.get("dataGeracao"),
                "dados_brutos": p,
            }
            lista_processos.append(proc_formatado)

        return lista_processos, total_itens

    def listar_processos_caixa(
        self,
        apenas_meus: bool = False,
        usuario_id: int | str | None = None,
        limit: int = 100,
        start: int = 0,
    ) -> list[dict[str, Any]]:
        """List processes from a single page of the unit inbox."""
        processos, _ = self._buscar_pagina_caixa(
            apenas_meus=apenas_meus,
            usuario_id=usuario_id,
            limit=limit,
            start=start,
        )
        return processos

    def iterar_paginas_caixa(
        self,
        apenas_meus: bool = False,
        usuario_id: int | str | None = None,
        limit: int = 100,
        start: int = 0,
        max_paginas: int = 500,
    ) -> Iterator[list[dict[str, Any]]]:
        """Yield deduplicated inbox pages cleanly until exhausted or max_paginas is reached."""
        vistos: set[str] = set()
        tam_pagina = max(1, limit)

        for pagina in range(start, start + max_paginas):
            itens_pagina, total_itens = self._buscar_pagina_caixa(
                apenas_meus=apenas_meus,
                usuario_id=usuario_id,
                limit=tam_pagina,
                start=pagina,
            )
            if not itens_pagina:
                break

            novos: list[dict[str, Any]] = []
            for item in itens_pagina:
                chave = str(item.get("id_procedimento_sei") or item.get("numero_processo_sei") or "")
                if chave and chave in vistos:
                    continue
                if chave:
                    vistos.add(chave)
                novos.append(item)

            if not novos:
                break

            yield novos

            if total_itens > 0 and len(vistos) >= total_itens:
                break
            if total_itens == 0 and len(itens_pagina) < tam_pagina:
                break

    def consultar_processo_completo(self, protocolo_ou_id: str) -> dict[str, Any]:
        """Query full process details and document tree from SEI."""
        endpoint = f"/processo/consultar/{protocolo_ou_id.strip()}"
        response = self.executar_requisicao("GET", endpoint)

        if response.status_code != HTTPStatus.OK:
            raise RuntimeError(
                f"Falha ao consultar processo {protocolo_ou_id} ({response.status_code}): {response.text}"
            )

        data = response.json()
        if not data.get("sucesso"):
            raise RuntimeError(f"Erro ao consultar processo {protocolo_ou_id}: {data.get('mensagem')}")

        return data.get("data", {})

    def listar_documentos_processo(self, id_procedimento: int | str) -> list[dict[str, Any]]:
        """List all documents attached to a process."""
        endpoint = f"/documento/listar/{id_procedimento}"
        response = self.executar_requisicao("GET", endpoint)

        if response.status_code != HTTPStatus.OK:
            raise RuntimeError(
                f"Falha ao listar documentos do processo {id_procedimento} ({response.status_code}): {response.text}"
            )

        data = response.json()
        if not data.get("sucesso"):
            raise RuntimeError(f"Erro ao listar documentos do processo {id_procedimento}: {data.get('mensagem')}")

        documentos_brutos = data.get("data", [])
        if isinstance(documentos_brutos, dict):
            documentos_brutos = documentos_brutos.get("documentos", [])

        lista_docs = []
        for d in documentos_brutos:
            atributos = d.get("atributos", {}) or {}
            status_doc = atributos.get("status", {}) if isinstance(atributos.get("status"), dict) else {}
            id_doc = str(d.get("idProtocolo") or d.get("id") or atributos.get("idProtocolo") or "")
            proto_doc = str(
                atributos.get("protocoloFormatado")
                or d.get("protocoloFormatado")
                or d.get("documentoFormatado")
                or ""
            ).strip()
            lista_docs.append(
                {
                    "id_documento_sei": id_doc,
                    "id_procedimento_sei": str(id_procedimento),
                    "protocolo_formatado": proto_doc,
                    "tipo_documento": str(atributos.get("tipoDocumento", "")),
                    "mime_type": str(atributos.get("mimeType", "")),
                    "nome_composto": str(atributos.get("nomeComposto", "")),
                    "tipo": str(atributos.get("tipo", "")),
                    "cancelado": str(status_doc.get("documentoCancelado", "")).upper() == "S",
                    "dados_brutos": d,
                }
            )

        return lista_docs

    def stream_anexo_unico(
        self,
        client: httpx.Client,
        url: str,
        id_documento: int | str,
        headers: dict[str, str],
        max_bytes: int,
        req_timeout: httpx.Timeout,
    ) -> tuple[int, bytes | None]:
        """Executa uma tentativa de streaming de anexo dentro do contexto aberto."""
        chunks: list[bytes] = []
        total_bytes = 0

        with client.stream("GET", url, headers=headers, timeout=req_timeout) as response:
            if response.status_code in (HTTPStatus.UNAUTHORIZED, HTTPStatus.FORBIDDEN):
                return response.status_code, None

            if response.status_code in STATUS_RETENTAVEIS:
                return response.status_code, None

            if response.status_code != HTTPStatus.OK:
                raise RuntimeError(f"Falha ao baixar anexo {id_documento} no mod-wssei ({response.status_code})")

            primeiro_chunk = True
            for chunk in response.iter_bytes(chunk_size=CHUNK_SIZE):
                if not chunk:
                    continue

                if primeiro_chunk:
                    primeiro_chunk = False
                    if b'"sucesso":false' in chunk or (b"{" in chunk and b'"mensagem"' in chunk):
                        raise RuntimeError(
                            f"O SEI retornou erro ao baixar anexo {id_documento}: "
                            f"{chunk.decode('utf-8', errors='ignore')}"
                        )

                total_bytes += len(chunk)
                if total_bytes > max_bytes:
                    raise ValueError(
                        f"Documento {id_documento} excede o limite maximo permitido "
                        f"({total_bytes} > {max_bytes} bytes / {max_bytes / (1024 * 1024):.1f} MB)."
                    )
                chunks.append(chunk)

        return HTTPStatus.OK, b"".join(chunks)

    def baixar_documento(
        self,
        id_documento: int | str,
        max_bytes: int = MAX_DOCUMENT_BYTES,
        timeout: int | float | None = None,
    ) -> bytes:
        """Download document bytes using chunked streaming with memory cap and validation.

        Enforces streaming chunk iteration, checks PDF magic bytes and bounds memory.
        """
        endpoint = f"/documento/baixar/anexo/{id_documento}"
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        req_timeout = httpx.Timeout(float(timeout or self.config.timeout), connect=5.0)
        client = self.get_http_client()
        logger.info("Baixando documento %s via mod-wssei em streaming chunked...", id_documento)

        for tentativa, atraso in enumerate((*ATRASOS_RETENTATIVA, None)):
            headers = self.obter_headers()

            try:
                status_code, conteudo_bytes = self.stream_anexo_unico(
                    client=client,
                    url=url,
                    id_documento=id_documento,
                    headers=headers,
                    max_bytes=max_bytes,
                    req_timeout=req_timeout,
                )

                if status_code in (HTTPStatus.UNAUTHORIZED, HTTPStatus.FORBIDDEN):
                    logger.warning("Token expirado ao baixar anexo %s. Reautenticando...", id_documento)
                    self.reautenticar_se_necessario(headers.get("token"))
                    headers = self.obter_headers()
                    status_code, conteudo_bytes = self.stream_anexo_unico(
                        client=client,
                        url=url,
                        id_documento=id_documento,
                        headers=headers,
                        max_bytes=max_bytes,
                        req_timeout=req_timeout,
                    )

                if status_code in STATUS_RETENTAVEIS and atraso is not None:
                    logger.warning(
                        "SEI respondeu HTTP %d ao baixar anexo %s; retentativa %d/%d em %.1fs",
                        status_code,
                        id_documento,
                        tentativa + 1,
                        len(ATRASOS_RETENTATIVA),
                        atraso,
                    )
                    time.sleep(atraso)
                    continue

                if conteudo_bytes is None:
                    raise RuntimeError(f"Falha ao baixar anexo {id_documento} no mod-wssei ({status_code})")

                if not conteudo_bytes.startswith(b"%PDF-"):
                    logger.warning(
                        "Anexo %s baixado (%d bytes) nao inicia com magic bytes %%PDF-.",
                        id_documento,
                        len(conteudo_bytes),
                    )

                logger.info(
                    "Documento %s baixado com sucesso: %d bytes (%.1f KB).",
                    id_documento,
                    len(conteudo_bytes),
                    len(conteudo_bytes) / 1024.0,
                )
                return conteudo_bytes
            except httpx.TransportError as exc:
                if atraso is None:
                    raise RuntimeError(f"Erro de transporte HTTP ao baixar anexo {id_documento}: {exc}") from exc
                logger.warning(
                    "Erro de transporte ao baixar anexo %s (%s); retentando em %.1fs", id_documento, exc, atraso
                )
                time.sleep(atraso)
            except Exception as exc:
                if atraso is not None and "504" in str(exc):
                    logger.warning(
                        "Falha temporária ao baixar anexo %s (%s); retentando em %.1fs", id_documento, exc, atraso
                    )
                    time.sleep(atraso)
                    continue
                logger.error("Erro durante o download em streaming do anexo %s: %s", id_documento, exc)
                raise

        raise RuntimeError(f"Falha ao baixar anexo {id_documento} apos esgotar retentativas.")

    def baixar_anexo_seguro(self, id_documento: str, id_procedimento: int | str) -> tuple[str, bytes | None]:
        """Baixa anexo individual registrando aviso sem interromper os demais anexos."""
        try:
            return id_documento, self.baixar_documento(id_documento)
        except Exception as exc:
            logger.warning(
                "Falha ao unir anexo %s do processo %s: %s",
                id_documento,
                id_procedimento,
                exc,
            )
            return id_documento, None

    def obter_pdf_recurso_com_metadados(self, id_procedimento: int | str) -> tuple[str, str | None, bytes]:
        """Locate citizen appeal documents and return (ids_csv, protocolo_documento_sei, pdf_bytes)."""
        documentos = self.listar_documentos_processo(id_procedimento)

        candidatos: list[dict[str, Any]] = []
        for d in documentos:
            tipo_doc = str(d.get("tipo_documento", "")).upper()
            mime = str(d.get("mime_type", "")).lower()
            nome = str(d.get("nome_composto", "")).lower()
            tipo = str(d.get("tipo", "")).lower()

            if (
                tipo_doc in ("E", "X")
                or "pdf" in mime
                or "recurso" in nome
                or "recurso" in tipo
                or "anexo" in nome
                or "requerimento" in nome
            ):
                candidatos.append(d)

        nao_cancelados = [d for d in candidatos if not d.get("cancelado")]
        if nao_cancelados:
            candidatos = nao_cancelados

        if not candidatos and documentos:
            candidatos = [d for d in documentos if not d.get("cancelado")] or [documentos[0]]

        if not candidatos:
            raise FileNotFoundError(f"Nenhum documento anexado localizado para o processo {id_procedimento}.")

        proto_por_id = {
            str(d["id_documento_sei"]): str(d.get("protocolo_formatado") or "").strip() or None
            for d in candidatos
        }

        if len(candidatos) == 1:
            id_doc = str(candidatos[0]["id_documento_sei"])
            pdf_bytes = self.baixar_documento(id_doc)
            return id_doc, proto_por_id.get(id_doc), pdf_bytes

        # Multiplos anexos: baixa em streaming paralelo (pool limitado) e une na ordem via PyMuPDF
        logger.info(
            "Processo %s possui %d anexos. Consolidando em fluxo unico em RAM...",
            id_procedimento,
            len(candidatos),
        )
        ids_candidatos = [str(d["id_documento_sei"]) for d in candidatos]
        with ThreadPoolExecutor(max_workers=min(3, len(ids_candidatos))) as executor:
            anexos_baixados = list(
                executor.map(lambda id_d: self.baixar_anexo_seguro(id_d, id_procedimento), ids_candidatos)
            )

        ids_docs_baixados: list[str] = []
        with FITZ_LOCK:
            doc_consolidado = fitz.open()
            try:
                for id_d, conteudo in anexos_baixados:
                    if conteudo is None:
                        continue
                    try:
                        doc_parte = fitz.open(stream=conteudo, filetype="pdf")
                        try:
                            doc_consolidado.insert_pdf(doc_parte)
                        finally:
                            doc_parte.close()
                        ids_docs_baixados.append(id_d)
                    except Exception as exc:
                        logger.warning(
                            "Falha ao unir anexo %s do processo %s: %s",
                            id_d,
                            id_procedimento,
                            exc,
                        )

                if len(doc_consolidado) == 0:
                    raise RuntimeError(f"Nenhum PDF valido pode ser baixado para o processo {id_procedimento}.")

                pdf_bytes = doc_consolidado.tobytes(garbage=4, deflate=True)
            finally:
                doc_consolidado.close()

        proto_principal = next(
            (proto_por_id.get(id_d) for id_d in ids_docs_baixados if proto_por_id.get(id_d)),
            None,
        )
        return ",".join(ids_docs_baixados), proto_principal, pdf_bytes

    def obter_pdf_recurso_processo(self, id_procedimento: int | str) -> tuple[str, bytes]:
        """Locate citizen appeal documents and return consolidated binary PDF bytes in RAM."""
        ids_csv, _, pdf_bytes = self.obter_pdf_recurso_com_metadados(id_procedimento)
        return ids_csv, pdf_bytes
