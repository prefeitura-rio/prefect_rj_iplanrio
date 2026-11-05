"""Servidor Mock da API REST do SEI-Rio (mod-wssei) para testes locais, bancada e CI/CD.

Descrição Técnica & Arquitetura:
Simula fielmente a interface HTTP exposta pelo módulo `mod-wssei` v2 da Prefeitura do Rio.
Permite executar a esteira de triagem ponta a ponta sem dependência de credenciais
reais de produção ou conectividade com a rede corporativa. Implementa os 4 endpoints críticos:
1. `POST /autenticar`: valida usuário/senha e emite token JWT com expiração.
2. `GET /processo/listar`: lista a caixa JARI simulando 5 processos administrativos,
   suportando filtro por relator nominal (`apenasMeus='S'`) ou visão integral de Gestor.
3. `GET /documento/listar/{procedimento}`: retorna os metadados dos documentos do processo
   e o identificador do anexo de petição recursal externa (`tipoDocumento='E'`).
4. `GET /documento/baixar/anexo/{protocolo}`: serve stream binário real de PDFs de amostra
   armazenados em `pdfs/` (`00.pdf` a `04.pdf`) com cabeçalhos HTTP adequados.

Fluxo de Dependências:
- Consumidores Diretos:
  * `utils/sei_client.py`: atua como cliente HTTP consumindo os endpoints deste servidor.
  * `flow.py`, `scripts/run_local.py`: execução da esteira completa em modo local integrado.
- Dependências Diretas:
  * FastAPI e Uvicorn para o servidor REST assíncrono.
  * Pasta `pdfs/`: repositório das amostras de documentos servidas para download.
"""

import os
from http import HTTPStatus
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs

import uvicorn
from fastapi import FastAPI, Header, HTTPException, Query, Request, Response

try:
    from prefect_rj_iplanrio.logging import get_logger

    logger = get_logger(__name__)
except ImportError:
    import logging

    logger = logging.getLogger(__name__)

app = FastAPI(
    title="Simulador SEI-Rio (mod-wssei Mock)",
    description="Mock REST para desenvolvimento e testes da Pipeline SMTR e MCP Claude",
    version="1.0.0",
)

# Diretório base de PDFs para servir nos downloads de anexo
BASE_DIR = Path(__file__).resolve().parent.parent
PDFS_DIR = BASE_DIR / "pdfs"

# Amostra de 5 processos para teste
MOCK_PROCESSOS = [
    {
        "id": "10001",
        "atributos": {
            "idProcedimento": "10001",
            "numero": "2026-00001/JARI",
            "tipoProcesso": "Recurso de Multa",
            "unidade": {"idUnidade": "110000001", "sigla": "SMTR/1_JARI"},
            "usuarioAtribuido": {
                "idUsuario": "1001",
                "sigla": "carlos.relator",
                "nome": "Carlos Relator (1_JARI)",
            },
            "status": {"processoAberto": "S"},
        },
        "pdf_vinculado": "00.pdf",
    },
    {
        "id": "10002",
        "atributos": {
            "idProcedimento": "10002",
            "numero": "2026-00002/JARI",
            "tipoProcesso": "Recurso de Multa",
            "unidade": {"idUnidade": "110000001", "sigla": "SMTR/1_JARI"},
            "usuarioAtribuido": {
                "idUsuario": "1001",
                "sigla": "carlos.relator",
                "nome": "Carlos Relator (1_JARI)",
            },
            "status": {"processoAberto": "S"},
        },
        "pdf_vinculado": "01.pdf",
    },
    {
        "id": "10003",
        "atributos": {
            "idProcedimento": "10003",
            "numero": "2026-00003/JARI",
            "tipoProcesso": "Recurso de Multa",
            "unidade": {"idUnidade": "110000001", "sigla": "SMTR/1_JARI"},
            "usuarioAtribuido": {
                "idUsuario": "1002",
                "sigla": "ana.relatora",
                "nome": "Ana Relatora (1_JARI)",
            },
            "status": {"processoAberto": "S"},
        },
        "pdf_vinculado": "02.pdf",
    },
    {
        "id": "10004",
        "atributos": {
            "idProcedimento": "10004",
            "numero": "2026-00004/JARI",
            "tipoProcesso": "Recurso de Multa",
            "unidade": {"idUnidade": "110000001", "sigla": "SMTR/1_JARI"},
            "usuarioAtribuido": {
                "idUsuario": "1002",
                "sigla": "ana.relatora",
                "nome": "Ana Relatora (1_JARI)",
            },
            "status": {"processoAberto": "S"},
        },
        "pdf_vinculado": "03.pdf",
    },
    {
        "id": "10005",
        "atributos": {
            "idProcedimento": "10005",
            "numero": "2026-00005/JARI",
            "tipoProcesso": "Recurso de Multa",
            "unidade": {"idUnidade": "110000001", "sigla": "SMTR/1_JARI"},
            "usuarioAtribuido": None,
            "status": {"processoAberto": "S"},
        },
        "pdf_vinculado": "04.pdf",
    },
]

VALID_TOKEN = "mock-token-sei-smtr-2026"


def validar_token(token: str | None) -> None:
    """Valida a presença e validade do token de autenticação recebido no cabeçalho.

    :param token: String do token JWT recebida no header HTTP.
    :raises HTTPException: Se o token for inválido ou ausente.
    """
    if not token or token != VALID_TOKEN:
        raise HTTPException(
            status_code=HTTPStatus.UNAUTHORIZED,
            detail="Token inválido ou ausente.",
        )


@app.post("/autenticar")
async def autenticar(request: Request) -> dict[str, Any]:
    """Simula o endpoint POST /autenticar do mod-wssei.

    :param request: Objeto de requisição FastAPI com os dados de login.
    :returns: Resposta JSON contendo o token JWT simulado.
    """
    content_type = request.headers.get("content-type", "")
    if "json" in content_type:
        body = await request.json()
    else:
        raw_body = await request.body()
        parsed = parse_qs(raw_body.decode("utf-8", errors="ignore"))
        body = {k: v[0] for k, v in parsed.items()}

    usuario = body.get("usuario")
    senha = body.get("senha")
    orgao = int(body.get("orgao", 0))

    if not usuario or not senha:
        return {"sucesso": False, "mensagem": "Usuário e senha são obrigatórios."}

    logger.info("Mock SEI: autenticação solicitada para usuário=%s órgão=%d", usuario, orgao)
    return {
        "sucesso": True,
        "data": {
            "token": VALID_TOKEN,
            "expiracao": "2099-12-31T23:59:59Z",
            "usuario": usuario,
        },
    }


@app.get("/processo/listar")
def listar_processos(  # noqa: PLR0913, PLR0917
    token: str = Header(..., description="Token JWT retornado pelo login"),
    unidade: int | None = Header(None, description="Id da unidade JARI"),
    apenas_meus: str | None = Query(None, alias="apenasMeus", description="'S' para apenas os do relator"),
    usuario: int | None = Query(None, description="ID do usuário atribuído"),
    limit: int = Query(100),
    start: int = Query(0),
) -> dict[str, Any]:
    """Simula o endpoint GET /processo/listar (Visão Gestor vs Jarista).

    :param token: Cabeçalho de autorização.
    :param unidade: ID numérico da unidade de sessão.
    :param apenas_meus: Filtro para restringir à própria atribuição ('S').
    :param usuario: ID do usuário atribuído específico.
    :param limit: Quantidade máxima de processos por página.
    :param start: Offset de paginação.
    :returns: Resposta formatada do SEI com a lista de processos da caixa.
    """
    validar_token(token)

    logger.info(
        "Mock SEI: listar_processos unidade=%s apenas_meus=%s usuario=%s",
        unidade,
        apenas_meus,
        usuario,
    )
    processos = MOCK_PROCESSOS

    if apenas_meus == "S" or usuario is not None:
        target_uid = str(usuario) if usuario else "1001"
        processos = [
            p
            for p in processos
            if p["atributos"]["usuarioAtribuido"] and str(p["atributos"]["usuarioAtribuido"]["idUsuario"]) == target_uid
        ]

    total = len(processos)
    paged = processos[start : start + limit]

    return {
        "sucesso": True,
        "total": str(total),
        "data": [
            {
                "id": p["id"],
                "atributos": p["atributos"],
            }
            for p in paged
        ],
    }


@app.get("/documento/listar/{procedimento}")
def listar_documentos_processo(
    procedimento: str,
    token: str = Header(...),
    unidade: int | None = Header(None),
) -> dict[str, Any]:
    """Simula o endpoint GET /documento/listar/{procedimento}.

    :param procedimento: ID do processo no SEI.
    :param token: Token de autenticação.
    :param unidade: Unidade ativa na requisição.
    :returns: Resposta JSON contendo a lista de documentos do processo.
    :raises HTTPException: Se o processo não for encontrado.
    """
    validar_token(token)

    proc_achado = next((p for p in MOCK_PROCESSOS if p["id"] == str(procedimento)), None)
    if not proc_achado:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail=f"Processo {procedimento} não encontrado (unidade={unidade}).",
        )

    doc_id = f"doc_{procedimento}"
    return {
        "sucesso": True,
        "total": "1",
        "data": [
            {
                "id": doc_id,
                "atributos": {
                    "idProtocolo": doc_id,
                    "protocoloFormatado": f"DOC-{procedimento}",
                    "tipo": "Recurso de Multa (Petição Inicial)",
                    "tipoDocumento": "E",
                    "mimeType": "pdf",
                    "nomeComposto": f"Petição Recurso ({proc_achado['pdf_vinculado']})",
                },
            }
        ],
    }


@app.get("/documento/baixar/anexo/{protocolo}")
def baixar_anexo_documento(
    protocolo: str,
    token: str = Header(...),
    unidade: int | None = Header(None),
) -> Response:
    """Simula o endpoint GET /documento/baixar/anexo/{protocolo} devolvendo stream real.

    :param protocolo: ID do documento para download.
    :param token: Token de autenticação.
    :param unidade: Unidade da sessão.
    :returns: Response com os bytes do PDF e headers correspondentes.
    :raises HTTPException: Se o arquivo correspondente não existir.
    """
    validar_token(token)

    proc_id = protocolo.replace("doc_", "")
    proc_achado = next((p for p in MOCK_PROCESSOS if p["id"] == proc_id), None)

    nome_pdf = proc_achado["pdf_vinculado"] if proc_achado else "00.pdf"
    caminho_pdf = PDFS_DIR / nome_pdf

    if not caminho_pdf.exists():
        pdfs_existentes = list(PDFS_DIR.glob("*.pdf"))
        if not pdfs_existentes:
            raise HTTPException(
                status_code=HTTPStatus.NOT_FOUND,
                detail=f"Nenhum PDF disponível no servidor mock para unidade {unidade}.",
            )
        caminho_pdf = pdfs_existentes[0]

    pdf_bytes = caminho_pdf.read_bytes()

    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={
            "Content-Disposition": f"attachment; filename={nome_pdf}",
            "Content-Length": str(len(pdf_bytes)),
        },
    )


if __name__ == "__main__":
    porta = int(os.getenv("PORT", "8000"))
    logger.info("Iniciando Mock SEI-Rio (mod-wssei) em http://localhost:%d", porta)
    uvicorn.run(app, host="0.0.0.0", port=porta)
