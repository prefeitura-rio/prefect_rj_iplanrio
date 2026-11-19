"""SMTR triage adapter over the decoupled ``sei-client-smtr`` REST client.

Uses ``sei_client_smtr.SeiClientSync`` automatically when installed, and falls
back transparently to ``helpers/_sei_fallback.FallbackSeiSync`` when running in
environments without access to the private ``sei-client-smtr`` repository.
Keeps only SMTR-specific PDF appeal filtering and PyMuPDF merging in this module.
"""

from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Self

import httpx
import pymupdf as fitz

from pipelines.rj_smtr__triagem_recursos.constants import obter_env
from pipelines.rj_smtr__triagem_recursos.helpers._sei_fallback import (
    FallbackHooks,
    FallbackSeiSync,
)
from pipelines.rj_smtr__triagem_recursos.helpers.windowing import FITZ_LOCK
from prefect_rj_iplanrio.logging import get_logger

try:
    from sei_client_smtr import (
        PoolConfig,
        RetryConfig,
        SeiClientSync,
        SeiHooks,
        TimeoutConfig,
    )
    from sei_client_smtr import SeiConfig as CoreSeiConfig
    from sei_client_smtr.config import DEFAULT_MAX_DOWNLOAD_BYTES

    USANDO_CLIENTE_EXTERNO = True
except ImportError:
    SeiClientSync = None  # type: ignore[assignment,misc]
    CoreSeiConfig = None  # type: ignore[assignment,misc]
    DEFAULT_MAX_DOWNLOAD_BYTES = 30 * 1024 * 1024
    USANDO_CLIENTE_EXTERNO = False

logger = get_logger(__name__)

MAX_DOCUMENT_BYTES = DEFAULT_MAX_DOWNLOAD_BYTES  # 30 MB teto de memoria por documento


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
        env_base_url = obter_env("SEI_URL") or obter_env("SEI_API_BASE_URL") or ""
        env_usuario = obter_env("SEI_USUARIO") or obter_env("SEI_SERVICE_USER") or ""
        env_senha = obter_env("SEI_SENHA") or obter_env("SEI_SERVICE_PASSWORD") or ""
        env_unidade = obter_env("SEI_UNIDADE_ID") or obter_env("SEI_ID_UNIDADE") or ""
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

    def to_core_config(self, max_connections: int = 16) -> Any:
        """Converte a configuração da pipeline para ``sei_client_smtr.SeiConfig`` quando disponível."""
        if CoreSeiConfig is None:
            raise RuntimeError("Pacote externo 'sei-client-smtr' não instalado neste ambiente.")
        cap = max(4, max_connections)
        timeout_float = float(self.timeout)
        return CoreSeiConfig(
            url=self.base_url,
            usuario=self.usuario,
            senha=self.senha,
            orgao=str(self.orgao),
            unidade_id=self.id_unidade,
            timeout=TimeoutConfig(
                connect=5.0,
                read=timeout_float,
                write=min(timeout_float, 30.0),
                pool=10.0,
            ),
            pool=PoolConfig(
                max_connections=cap,
                max_keepalive_connections=max(4, min(cap, 12)),
                keepalive_expiry=30.0,
            ),
            retry=RetryConfig.batch(),
            verify_ssl=self.verify_ssl,
        )


def _processo_para_dict(proc: Any) -> dict[str, Any]:
    """Converte ``ProcessoSEI`` (externo ou fallback) para o dicionário esperado pela pipeline."""
    atributos = proc.raw.get("atributos") if isinstance(proc.raw.get("atributos"), dict) else {}
    id_doc = proc.raw.get("idDocumento") or atributos.get("idDocumento")
    usuario_atrib_sigla = None
    if proc.atribuido_unidade_atual is not None:
        usuario_atrib_sigla = proc.atribuido_unidade_atual.sigla or proc.atribuido_unidade_atual.nome or None
    marcador_nome = proc.marcador.nome if getattr(proc, "marcador", None) is not None else None

    return {
        "processo_id": proc.id_procedimento,
        "id_procedimento_sei": proc.id_procedimento,
        "numero_processo_sei": proc.protocolo,
        "numero_processo": proc.protocolo,
        "id_documento_sei": id_doc,
        "id_documento": id_doc,
        "sigla_unidade": proc.unidade_geradora_sigla or "10_JARI",
        "usuario_atribuido_sigla": usuario_atrib_sigla,
        "marcador": marcador_nome,
        "data_autuacao": proc.data_autuacao,
        "dados_brutos": proc.raw,
    }


def _documento_para_dict(doc: Any) -> dict[str, Any]:
    """Converte ``DocumentoSEI`` (externo ou fallback) para o dicionário legado da pipeline."""
    return {
        "id_documento_sei": doc.id,
        "id_procedimento_sei": doc.id_procedimento,
        "protocolo_formatado": doc.protocolo_formatado,
        "tipo_documento": doc.tipo_documento,
        "mime_type": doc.mime_type,
        "nome_composto": doc.nome_composto,
        "tipo": doc.tipo,
        "cancelado": doc.cancelado,
        "dados_brutos": doc.raw,
    }


class ModWsseiClient:
    """Adapter da pipeline SMTR sobre ``sei-client-smtr`` (ou fallback interno automático)."""

    def __init__(
        self,
        config: SeiConfig | None = None,
        base_url: str | None = None,
        is_local: bool = False,
        http_client: httpx.Client | None = None,
        max_connections: int = 16,
        usar_cliente_externo: bool | None = None,
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
        quer_externo = USANDO_CLIENTE_EXTERNO if usar_cliente_externo is None else usar_cliente_externo
        self._usando_externo = bool(quer_externo and SeiClientSync is not None)
        self._sei: Any = self._criar_backend(http_client=http_client, max_connections=max_connections)

    def _criar_backend(self, *, http_client: httpx.Client | None, max_connections: int) -> Any:
        if self._usando_externo and SeiClientSync is not None:
            hooks = SeiHooks(on_retry=self._hook_retry, on_response=self._hook_response)
            return SeiClientSync(
                self.config.to_core_config(max_connections=max_connections),
                http_client=http_client,
                hooks=hooks,
            )

        logger.debug("Pacote externo 'sei-client-smtr' indisponível; usando FallbackSeiSync interno.")
        return FallbackSeiSync(
            url=self.config.base_url,
            usuario=self.config.usuario,
            senha=self.config.senha,
            orgao=str(self.config.orgao),
            unidade_id=self.config.id_unidade,
            timeout=float(self.config.timeout),
            verify_ssl=self.config.verify_ssl,
            max_connections=max_connections,
            max_download_bytes=MAX_DOCUMENT_BYTES,
            http_client=http_client,
            hooks=FallbackHooks(on_retry=self._hook_retry, on_response=self._hook_response),
        )

    @staticmethod
    def _hook_retry(method: str, endpoint: str, attempt: int, status: int | None, delay: float) -> None:
        logger.warning(
            "SEI retentativa %d em %s %s (status=%s, aguardando %.2fs)",
            attempt,
            method,
            endpoint,
            status if status is not None else "erro_transporte",
            delay,
        )

    @staticmethod
    def _hook_response(method: str, endpoint: str, status_code: int, elapsed_ms: float) -> None:
        logger.debug("SEI %s %s -> HTTP %d (%.1f ms)", method, endpoint, status_code, elapsed_ms)

    @property
    def token(self) -> str | None:
        """Token JWT ativo da sessão atual."""
        return self._sei.token

    @token.setter
    def token(self, value: str | None) -> None:
        """Permite injeção direta de token (usado em testes de expiração 401)."""
        self._sei._token = value
        self._sei._sessao = None

    @property
    def sigla_usuario(self) -> str | None:
        """Sigla do usuário autenticado na última sessão."""
        return self._sei.sessao.sigla if self._sei.sessao else None

    @property
    def nome_usuario(self) -> str | None:
        """Nome completo do usuário autenticado na última sessão."""
        return self._sei.sessao.nome if self._sei.sessao else None

    @property
    def http_client(self) -> httpx.Client:
        """Expõe o pool HTTP persistente compartilhado pelo cliente."""
        return self._sei.http_client

    def get_http_client(self) -> httpx.Client:
        """Retorna o pool ``httpx.Client`` subjacente."""
        return self._sei.http_client

    def close(self) -> None:
        """Fecha o pool HTTP compartilhado quando pertencente a esta instância."""
        self._sei.close()

    def __enter__(self) -> Self:
        """Suporta uso com gerenciador de contexto."""
        return self

    def __exit__(self, *args: object) -> None:
        """Garante liberação do pool HTTP ao sair do contexto."""
        self.close()

    def autenticar(
        self,
        usuario: str | None = None,
        senha: str | None = None,
        orgao: int | None = None,
    ) -> str:
        """Authenticate via ``POST /autenticar`` and return the JWT token string."""
        sessao = self._sei.autenticar(usuario=usuario, senha=senha, orgao=orgao)
        return str(sessao.token)

    def executar_requisicao(
        self,
        metodo: str,
        endpoint: str,
        params: dict[str, Any] | None = None,
        json_data: dict[str, Any] | None = None,
        timeout: int | float | None = None,
    ) -> httpx.Response:
        """Delegate an authenticated HTTP call to the active SEI backend."""
        metodo_up = metodo.upper()
        return self._sei._executar_http_com_reauth(
            metodo_up,
            endpoint,
            params=params,
            json_data=json_data,
            timeout=float(timeout) if timeout is not None else None,
            operacao=f"{metodo_up} {endpoint}",
        )

    def listar_pagina_caixa_com_total(
        self,
        apenas_meus: bool = False,
        usuario_id: int | str | None = None,
        limit: int = 100,
        start: int = 0,
    ) -> tuple[list[dict[str, Any]], int]:
        """List a single page of the unit inbox returning (processes, total_itens)."""
        pagina = self._sei.listar_processos_pagina(
            limit=limit,
            start=start,
            apenas_meus=apenas_meus,
            usuario=usuario_id,
        )
        return [_processo_para_dict(proc) for proc in pagina.itens], int(pagina.total_itens)

    def listar_processos_caixa(
        self,
        apenas_meus: bool = False,
        usuario_id: int | str | None = None,
        limit: int = 100,
        start: int = 0,
    ) -> list[dict[str, Any]]:
        """List processes from a single page of the unit inbox."""
        processos, _ = self.listar_pagina_caixa_com_total(
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
        for pagina in self._sei.iterar_paginas_processos(
            limit=limit,
            start=start,
            max_paginas=max_paginas,
            apenas_meus=apenas_meus,
            usuario=usuario_id,
        ):
            yield [_processo_para_dict(proc) for proc in pagina.itens]

    def consultar_processo_completo(self, protocolo_ou_id: str) -> dict[str, Any]:
        """Query full process details and document tree from SEI."""
        chave = str(protocolo_ou_id).strip()
        if chave.isdigit():
            proc = self._sei.consultar_processo_por_id(chave)
        else:
            proc = self._sei.consultar_processo_completo(chave)
        return dict(proc.raw)

    def listar_documentos_processo(self, id_procedimento: int | str) -> list[dict[str, Any]]:
        """List all documents attached to a process (paginated)."""
        pagina = self._sei.listar_documentos_todos(id_procedimento)
        return [_documento_para_dict(doc) for doc in pagina.itens]

    def baixar_documento(
        self,
        id_documento: int | str,
        max_bytes: int = MAX_DOCUMENT_BYTES,
        timeout: int | float | None = None,
    ) -> bytes:
        """Download document bytes using chunked streaming with memory cap and validation."""
        conteudo_doc = self._sei.baixar_documento(
            id_documento,
            max_bytes=max_bytes,
            timeout=float(timeout) if timeout is not None else None,
        )
        if not conteudo_doc.is_pdf:
            logger.warning(
                "Anexo %s baixado (%d bytes) nao inicia com magic bytes %%PDF-.",
                id_documento,
                len(conteudo_doc.conteudo),
            )
        return bytes(conteudo_doc.conteudo)

    def baixar_anexo_seguro(self, id_documento: str, id_procedimento: int | str) -> tuple[str, bytes | None]:
        """Baixa anexo individual registrando aviso sem interromper os demais anexos."""
        try:
            return id_documento, self.baixar_documento(id_documento)
        except Exception as exc:
            logger.warning(
                "Falha ao baixar anexo %s do processo %s: %s",
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
