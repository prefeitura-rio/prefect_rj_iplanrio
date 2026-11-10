"""HTTP client for SEI-Rio integration via the mod-wssei REST API.

Provides streaming downloads with strict memory limits, token auto-renewal,
retries with backoff on 502/503/504, and process listing for JARI triage.
"""

from dataclasses import dataclass
from http import HTTPStatus
import os
import time
from typing import Any

import httpx
from prefect.logging import get_logger

logger = get_logger("toolkit")

MAX_DOCUMENT_BYTES = 30 * 1024 * 1024  # 30 MB teto de memoria por documento
CHUNK_SIZE = 64 * 1024  # 64 KB por chunk de streaming
STATUS_RETENTAVEIS = frozenset({HTTPStatus.BAD_GATEWAY, HTTPStatus.SERVICE_UNAVAILABLE, HTTPStatus.GATEWAY_TIMEOUT})
ATRASOS_RETENTATIVA = (0.5, 1.0, 2.0)


@dataclass(frozen=True)
class SeiConfig:
    """Connection and authentication configuration for mod-wssei REST API."""

    base_url: str = os.getenv("SEI_API_BASE_URL", "http://localhost:8000")
    usuario: str = os.getenv("SEI_SERVICE_USER", "usuario_servico")
    senha: str = os.getenv("SEI_SERVICE_PASSWORD", "senha_servico")
    id_unidade: str = os.getenv("SEI_ID_UNIDADE", "110000001")
    orgao: int = int(os.getenv("SEI_ORGAO", "12"))
    timeout: int = int(os.getenv("SEI_TIMEOUT", "60"))


class ModWsseiClient:
    """HTTP client communicating with the SEI-Rio REST API (mod-wssei)."""

    def __init__(
        self,
        config: SeiConfig | None = None,
        base_url: str | None = None,
    ) -> None:
        """Initialize the SEI client with config or custom base URL."""
        if config is not None:
            self.config = config
        elif base_url is not None:
            self.config = SeiConfig(base_url=base_url)
        else:
            self.config = SeiConfig()

        self.base_url = self.config.base_url.rstrip("/")
        self.token: str | None = None
        self.sigla_usuario: str | None = None
        self.nome_usuario: str | None = None

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
        with httpx.Client(timeout=self.config.timeout) as client:
            response = client.post(url, data=payload)

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
        req_timeout = timeout or self.config.timeout

        for tentativa, atraso in enumerate((*ATRASOS_RETENTATIVA, None)):
            headers = self.obter_headers()
            try:
                with httpx.Client(timeout=req_timeout) as client:
                    response = client.request(
                        method=metodo,
                        url=url,
                        headers=headers,
                        params=params,
                        json=json_data,
                    )

                if response.status_code in (HTTPStatus.UNAUTHORIZED, HTTPStatus.FORBIDDEN):
                    logger.warning("Token expirado (HTTP %d). Reautenticando...", response.status_code)
                    self.autenticar()
                    headers = self.obter_headers()
                    with httpx.Client(timeout=req_timeout) as client:
                        response = client.request(
                            method=metodo,
                            url=url,
                            headers=headers,
                            params=params,
                            json=json_data,
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

    def listar_processos_caixa(
        self,
        apenas_meus: bool = False,
        usuario_id: int | str | None = None,
        limit: int = 100,
        start: int = 0,
    ) -> list[dict[str, Any]]:
        """List processes currently present in the unit inbox with pagination."""
        endpoint = "/processo/listar"
        params: dict[str, Any] = {
            "limit": limit,
            "start": start,
            "apenasMeus": "S" if apenas_meus else "",
        }
        if usuario_id is not None:
            params["idUsuarioAtribuido"] = str(usuario_id)

        response = self.executar_requisicao("GET", endpoint, params=params)
        if response.status_code != HTTPStatus.OK:
            raise RuntimeError(f"Falha ao listar processos ({response.status_code}): {response.text}")

        data = response.json()
        if not data.get("sucesso"):
            raise RuntimeError(f"Erro retornado pelo SEI ao listar processos: {data.get('mensagem')}")

        processos_brutos = data.get("data", {}).get("processos", [])
        lista_processos = []

        for p in processos_brutos:
            proc_formatado = {
                "processo_id": p.get("idProcedimento"),
                "id_procedimento_sei": str(p.get("idProcedimento", "")),
                "numero_processo_sei": p.get("protocoloFormatado", ""),
                "numero_processo": p.get("protocoloFormatado", ""),
                "id_documento_sei": p.get("idDocumento"),
                "id_documento": p.get("idDocumento"),
                "sigla_unidade": p.get("siglaUnidade", "10_JARI"),
                "usuario_atribuido_sigla": p.get("usuarioAtribuido", {}).get("sigla")
                if isinstance(p.get("usuarioAtribuido"), dict)
                else None,
                "data_autuacao": p.get("dataGeracao"),
                "dados_brutos": p,
            }
            lista_processos.append(proc_formatado)

        return lista_processos

    def consultar_processo_completo(self, protocolo_ou_id: str) -> dict[str, Any]:
        """Query full process details and document tree from SEI."""
        endpoint = f"/processo/consultar/{protocolo_ou_id.strip()}"
        response = self.executar_requisicao("GET", endpoint)

        if response.status_code != HTTPStatus.OK:
            raise RuntimeError(f"Falha ao consultar processo {protocolo_ou_id} ({response.status_code}): {response.text}")

        data = response.json()
        if not data.get("sucesso"):
            raise RuntimeError(f"Erro ao consultar processo {protocolo_ou_id}: {data.get('mensagem')}")

        return data.get("data", {})

    def listar_documentos_processo(self, id_procedimento: int | str) -> list[dict[str, Any]]:
        """List all documents attached to a process."""
        endpoint = f"/procedimento/{id_procedimento}/documentos/todos"
        response = self.executar_requisicao("GET", endpoint)

        if response.status_code != HTTPStatus.OK:
            raise RuntimeError(
                f"Falha ao listar documentos do processo {id_procedimento} ({response.status_code}): {response.text}"
            )

        data = response.json()
        if not data.get("sucesso"):
            raise RuntimeError(f"Erro ao listar documentos do processo {id_procedimento}: {data.get('mensagem')}")

        documentos_brutos = data.get("data", {}).get("documentos", [])
        lista_docs = []

        for d in documentos_brutos:
            atributos = d.get("atributos", {})
            lista_docs.append(
                {
                    "id_documento_sei": str(d.get("idProtocolo", "")),
                    "id_procedimento_sei": str(id_procedimento),
                    "protocolo_formatado": str(atributos.get("protocoloFormatado", "")),
                    "tipo_documento": str(atributos.get("tipoDocumento", "")),
                    "mime_type": str(atributos.get("mimeType", "")),
                    "nome_composto": str(atributos.get("nomeComposto", "")),
                    "tipo": str(atributos.get("tipo", "")),
                    "dados_brutos": d,
                }
            )

        return lista_docs

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
        req_timeout = timeout or self.config.timeout
        logger.info("Baixando documento %s via mod-wssei em streaming chunked...", id_documento)

        headers = self.obter_headers()
        chunks: list[bytes] = []
        total_bytes = 0

        try:
            with httpx.Client(timeout=req_timeout) as client:
                with client.stream("GET", url, headers=headers) as response:
                    # Se expirado, tenta renovar token
                    if response.status_code in (HTTPStatus.UNAUTHORIZED, HTTPStatus.FORBIDDEN):
                        logger.warning("Token expirado ao baixar anexo %s. Reautenticando...", id_documento)
                        self.autenticar()
                        headers = self.obter_headers()
                        with client.stream("GET", url, headers=headers) as retry_response:
                            if retry_response.status_code != HTTPStatus.OK:
                                raise RuntimeError(
                                    f"Falha ao baixar anexo {id_documento} apos reautenticacao ({retry_response.status_code})"
                                )
                            response = retry_response  # noqa: PLW2901

                    if response.status_code != HTTPStatus.OK:
                        raise RuntimeError(
                            f"Falha ao baixar anexo {id_documento} no mod-wssei ({response.status_code})"
                        )

                    primeiro_chunk = True
                    for chunk in response.iter_bytes(chunk_size=CHUNK_SIZE):
                        if not chunk:
                            continue

                        if primeiro_chunk:
                            primeiro_chunk = False
                            # Valida se a resposta e um JSON de erro disfarcado
                            if b'"sucesso":false' in chunk or (b"{" in chunk and b'"mensagem"' in chunk):
                                response.close()
                                raise RuntimeError(
                                    f"O SEI retornou erro ao baixar anexo {id_documento}: {chunk.decode('utf-8', errors='ignore')}"
                                )

                        total_bytes += len(chunk)
                        if total_bytes > max_bytes:
                            response.close()
                            raise ValueError(
                                f"Documento {id_documento} excede o limite maximo permitido "
                                f"({total_bytes} > {max_bytes} bytes / {max_bytes / (1024*1024):.1f} MB)."
                            )
                        chunks.append(chunk)

            conteudo_bytes = b"".join(chunks)

            # Valida cabecalho minimo do arquivo binario
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
        except Exception as exc:
            logger.error("Erro durante o download em streaming do anexo %s: %s", id_documento, exc)
            raise

    def obter_pdf_recurso_processo(self, id_procedimento: int | str) -> tuple[str, bytes]:
        """Locate citizen appeal documents and return consolidated binary PDF bytes in RAM."""
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

        if not candidatos and documentos:
            candidatos = [documentos[0]]

        if not candidatos:
            raise FileNotFoundError(f"Nenhum documento anexado localizado para o processo {id_procedimento}.")

        if len(candidatos) == 1:
            id_doc = str(candidatos[0]["id_documento_sei"])
            pdf_bytes = self.baixar_documento(id_doc)
            return id_doc, pdf_bytes

        # Multiplos anexos: baixa em streaming na RAM e une via PyMuPDF
        import fitz

        logger.info(
            "Processo %s possui %d anexos. Consolidando em fluxo unico em RAM...",
            id_procedimento,
            len(candidatos),
        )
        doc_consolidado = fitz.open()
        ids_docs_baixados: list[str] = []

        for d in candidatos:
            id_d = str(d["id_documento_sei"])
            try:
                b = self.baixar_documento(id_d)
                doc_parte = fitz.open(stream=b, filetype="pdf")
                doc_consolidado.insert_pdf(doc_parte)
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
            doc_consolidado.close()
            raise RuntimeError(f"Nenhum PDF valido pode ser baixado para o processo {id_procedimento}.")

        pdf_bytes = doc_consolidado.tobytes(garbage=4, deflate=True)
        doc_consolidado.close()
        return ",".join(ids_docs_baixados), pdf_bytes
