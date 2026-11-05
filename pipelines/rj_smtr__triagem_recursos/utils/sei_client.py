"""Cliente HTTP REST para integração com o SEI-Rio através do módulo mod-wssei.

Descrição Técnica & Arquitetura:
Fornece a interface de comunicação de rede com o Sistema Eletrônico de Informações (SEI-Rio)
via API REST exposta pelo módulo `mod-wssei`. Gerencia a autenticação com obtenção de token JWT,
renovação automática de credenciais expiradas (HTTP 401/403), paginação de processos
na caixa da JARI (com distinção entre visão de Gestor e visão de Jarista/relator), listagem
de anexos processuais e download de petições recursais em streaming binário diretamente
na memória RAM, garantindo que nenhum documento sensível seja persistido em disco (LGPD).

Fluxo de Dependências:
- Consumidores Diretos:
  * `flow.py`: instancia `ModWsseiClient` automaticamente quando o parâmetro `fonte_pdf="sei"`.
  * `utils/db_repository.py`: consome `listar_processos_caixa` para identificar recursos pendentes.
  * `utils/storage.py` & `utils/triage.py`: invoca `obter_pdf_recurso_processo` e `baixar_documento`
    para receber os bytes do PDF na memória RAM durante a análise.
  * `scripts/mock_sei_server.py`: serve como contraparte de simulação dos mesmos contratos de endpoints.
- Dependências Diretas:
  * `prefect_rj_iplanrio.logging`: logger pré-configurado com integração OpenTelemetry.
  * Módulo de rede HTTP (`requests`) com tratamento de exceções de conexão e timeouts.
"""

import os
from dataclasses import dataclass
from http import HTTPStatus
from typing import Any

import requests

try:
    from prefect_rj_iplanrio.logging import get_logger

    logger = get_logger(__name__)
except ImportError:
    import logging

    logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SeiConfig:
    """Configuração de conexão e autenticação para a API REST mod-wssei.

    Agrupa parâmetros de conexão seguindo o guia de estilo do repositório.
    """

    base_url: str = os.getenv("SEI_API_BASE_URL", "http://localhost:8000")
    usuario: str = os.getenv("SEI_SERVICE_USER", "usuario_servico")
    senha: str = os.getenv("SEI_SERVICE_PASSWORD", "senha_servico")
    id_unidade: str = os.getenv("SEI_ID_UNIDADE", "110000001")
    orgao: int = 0
    timeout: int = 30


class ModWsseiClient:
    """Cliente HTTP para comunicação com a API REST do SEI-Rio (mod-wssei)."""

    def __init__(
        self,
        config: SeiConfig | None = None,
        base_url: str | None = None,
    ) -> None:
        """Inicializa o cliente com os parâmetros de configuração.

        :param config: Objeto SeiConfig com as credenciais e URLs.
        :param base_url: URL base alternativa (se config não for fornecido).
        """
        if config is not None:
            self.config = config
        elif base_url is not None:
            self.config = SeiConfig(base_url=base_url)
        else:
            self.config = SeiConfig()

        self.base_url = self.config.base_url.rstrip("/")
        self.token: str | None = None

    def autenticar(
        self,
        usuario: str | None = None,
        senha: str | None = None,
        orgao: int | None = None,
    ) -> str:
        """Obtém o token JWT de autenticação através do endpoint POST /autenticar.

        :param usuario: Nome de usuário opcional (se None, usa a config).
        :param senha: Senha opcional (se None, usa a config).
        :param orgao: ID do órgão opcional (se None, usa a config).
        :returns: String do token JWT recebido do SEI.
        :raises RuntimeError: Se a autenticação falhar ou a resposta indicar erro.
        """
        user = usuario or self.config.usuario
        pwd = senha or self.config.senha
        org = self.config.orgao if orgao is None else orgao

        url = f"{self.base_url}/autenticar"
        payload = {
            "usuario": user,
            "senha": pwd,
            "orgao": org,
        }

        logger.info("Autenticando no mod-wssei em: %s (usuário: %s)", url, user)
        response = requests.post(url, data=payload, timeout=self.config.timeout)

        if response.status_code != HTTPStatus.OK:
            raise RuntimeError(f"Falha na autenticação do SEI ({response.status_code}): {response.text}")

        data = response.json()
        if not data.get("sucesso"):
            raise RuntimeError(f"O SEI recusou a autenticação: {data.get('mensagem', data)}")

        token = data.get("data", {}).get("token")
        if not token:
            raise RuntimeError("Resposta do SEI não continha o campo 'token'.")

        self.token = token
        logger.info("Autenticação no mod-wssei concluída com sucesso.")
        return token

    def obter_headers(self) -> dict[str, str]:
        """Gera os headers HTTP padrões incluindo token e unidade de sessão.

        :returns: Dicionário com cabeçalhos HTTP necessários.
        """
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
        stream: bool = False,
    ) -> requests.Response:
        """Executa requisição com suporte a auto-renovação de token em caso de HTTP 401.

        :param metodo: Método HTTP ('GET', 'POST', etc.).
        :param endpoint: Rota da API (ex.: '/processo/listar').
        :param params: Parâmetros de query string.
        :param json_data: Payload JSON opcional.
        :param stream: Se True, mantém o socket aberto para streaming binário.
        :returns: Objeto Response da biblioteca requests.
        :raises RuntimeError: Se a requisição falhar após tentativa de reconexão.
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        headers = self.obter_headers()

        response = requests.request(
            method=metodo,
            url=url,
            headers=headers,
            params=params,
            json=json_data,
            stream=stream,
            timeout=self.config.timeout,
        )

        # Se o token expirou (HTTP 401 ou 403), reautentica e tenta mais uma vez
        if response.status_code in (HTTPStatus.UNAUTHORIZED, HTTPStatus.FORBIDDEN):
            logger.warning(
                "Token expirado ou inválido (HTTP %d). Reautenticando...",
                response.status_code,
            )
            self.autenticar()
            headers = self.obter_headers()
            response = requests.request(
                method=metodo,
                url=url,
                headers=headers,
                params=params,
                json=json_data,
                stream=stream,
                timeout=self.config.timeout,
            )

        return response

    def listar_processos_caixa(
        self,
        apenas_meus: bool = False,
        usuario_id: int | str | None = None,
        limit: int = 100,
        start: int = 0,
    ) -> list[dict[str, Any]]:
        """Lista os processos presentes na caixa da JARI.

        Na visão do Gestor (apenas_meus=False), lista todos os processos da unidade.
        Na visão do Jarista (apenas_meus=True), filtra apenas os atribuídos a ele.

        :param apenas_meus: Se True, envia apenasMeus='S' para filtrar pelo relator logado.
        :param usuario_id: ID numérico opcional de usuário atribuído para filtro específico.
        :param limit: Quantidade máxima de processos por página.
        :param start: Offset para paginação.
        :returns: Lista de dicionários representando cada processo retornado pelo SEI.
        :raises RuntimeError: Se a chamada à API retornar erro.
        """
        params: dict[str, Any] = {
            "limit": limit,
            "start": start,
        }
        if apenas_meus:
            params["apenasMeus"] = "S"
        if usuario_id is not None:
            params["usuario"] = usuario_id

        response = self.executar_requisicao("GET", "/processo/listar", params=params)
        if response.status_code != HTTPStatus.OK:
            raise RuntimeError(f"Erro ao listar processos no mod-wssei ({response.status_code}): {response.text}")

        payload = response.json()
        if not payload.get("sucesso", True):
            raise RuntimeError(f"O SEI retornou erro: {payload.get('mensagem')}")

        processos_brutos = payload.get("data", [])
        lista_formatada = []

        for p in processos_brutos:
            atributos = p.get("atributos", {})
            usuario_atrib = atributos.get("usuarioAtribuido") or {}

            lista_formatada.append(
                {
                    "id_procedimento_sei": str(atributos.get("idProcedimento") or p.get("id")),
                    "numero_processo_sei": str(atributos.get("numero", "")),
                    "tipo_processo": str(atributos.get("tipoProcesso", "")),
                    "id_unidade": str(atributos.get("unidade", {}).get("idUnidade", self.config.id_unidade)),
                    "sigla_unidade": str(atributos.get("unidade", {}).get("sigla", "")),
                    "usuario_atribuido_id": str(usuario_atrib.get("idUsuario") or ""),
                    "usuario_atribuido_sigla": str(usuario_atrib.get("sigla") or ""),
                    "dados_brutos": p,
                }
            )

        logger.info(
            "mod-wssei: %d processo(s) recuperado(s) na caixa (apenas_meus=%s).",
            len(lista_formatada),
            apenas_meus,
        )
        return lista_formatada

    def listar_documentos_processo(
        self,
        id_procedimento: int | str,
        limit: int = 100,
        start: int = 0,
    ) -> list[dict[str, Any]]:
        """Lista todos os documentos anexados a um processo no SEI.

        :param id_procedimento: Identificador numérico interno do processo no SEI.
        :param limit: Limite de registros por página.
        :param start: Offset da paginação.
        :returns: Lista de dicionários descrevendo cada documento do processo.
        :raises RuntimeError: Se a consulta falhar.
        """
        endpoint = f"/documento/listar/{id_procedimento}"
        params = {"limit": limit, "start": start}

        response = self.executar_requisicao("GET", endpoint, params=params)
        if response.status_code != HTTPStatus.OK:
            raise RuntimeError(
                f"Erro ao listar documentos do processo {id_procedimento} ({response.status_code}): {response.text}"
            )

        payload = response.json()
        docs_brutos = payload.get("data", [])
        lista_docs = []

        for d in docs_brutos:
            atributos = d.get("atributos", {})
            lista_docs.append(
                {
                    "id_documento_sei": str(atributos.get("idProtocolo") or d.get("id")),
                    "protocolo_formatado": str(atributos.get("protocoloFormatado", "")),
                    "tipo_documento": str(atributos.get("tipoDocumento", "")),
                    "mime_type": str(atributos.get("mimeType", "")),
                    "nome_composto": str(atributos.get("nomeComposto", "")),
                    "tipo": str(atributos.get("tipo", "")),
                    "dados_brutos": d,
                }
            )

        return lista_docs

    def baixar_documento(self, id_documento: int | str) -> bytes:
        """Realiza o download em streaming dos bytes de um documento em memória RAM.

        Cumpre a regra estrita de não salvar o PDF em disco nem no banco de dados.

        :param id_documento: Identificador do documento (idProtocolo no SEI).
        :returns: Stream binário com os bytes puros do PDF.
        :raises RuntimeError: Se o download falhar ou retornar status diferente de 200.
        """
        endpoint = f"/documento/baixar/anexo/{id_documento}"
        logger.info("Baixando documento %s via mod-wssei em memória RAM...", id_documento)

        response = self.executar_requisicao("GET", endpoint, stream=True)
        if response.status_code != HTTPStatus.OK:
            raise RuntimeError(
                f"Falha ao baixar anexo {id_documento} no mod-wssei ({response.status_code}): {response.text}"
            )

        conteudo_bytes = response.content
        logger.info(
            "Documento %s baixado com sucesso: %d bytes (%.1f KB).",
            id_documento,
            len(conteudo_bytes),
            len(conteudo_bytes) / 1024.0,
        )
        return conteudo_bytes

    def obter_pdf_recurso_processo(self, id_procedimento: int | str) -> tuple[str, bytes]:
        """Localiza o documento de recurso do cidadão e baixa seus bytes em RAM.

        Identifica automaticamente o anexo externo (tipoDocumento='E') do processo.

        :param id_procedimento: ID interno do processo no SEI.
        :returns: Tupla (id_documento_sei, pdf_bytes).
        :raises FileNotFoundError: Se nenhum documento de recurso for localizado.
        """
        documentos = self.listar_documentos_processo(id_procedimento)

        doc_alvo = None
        for d in documentos:
            tipo_doc = d.get("tipo_documento", "").upper()
            mime = d.get("mime_type", "").lower()
            nome = d.get("nome_composto", "").lower()
            tipo = d.get("tipo", "").lower()

            if tipo_doc == "E" or "pdf" in mime or "recurso" in nome or "recurso" in tipo:
                doc_alvo = d
                break

        if not doc_alvo and documentos:
            doc_alvo = documentos[0]

        if not doc_alvo:
            raise FileNotFoundError(f"Nenhum documento anexado localizado para o processo {id_procedimento}.")

        id_doc = doc_alvo["id_documento_sei"]
        pdf_bytes = self.baixar_documento(id_doc)
        return id_doc, pdf_bytes
