"""Módulo de Gestão de Contexto e Smart Windowing.

Responsável por truncar processos administrativos extensos diretamente em memória RAM,
isolando as primeiras páginas (núcleo da petição recursal) e descartando anexos 
probatórios pesados antes do envio aos modelos multimodais.
Suporta leitura a partir de Path local, str, bytes ou io.BytesIO.
"""

import io
from pathlib import Path
import pymupdf as fitz


def extrair_smart_window_pdf(
    pdf_origem: Path | str | bytes | io.BytesIO,
    max_paginas: int = 20,
) -> bytes:
    """Trunca um arquivo PDF até o limite estipulado de páginas operando em memória.

    :param pdf_origem: Caminho local (Path/str) OU stream binário (bytes/BytesIO).
    :param max_paginas: Quantidade máxima de páginas a reter (padrão: 20).
    :returns: Binário do sub-PDF truncado e compactado.
    :raises FileNotFoundError: Se o caminho do PDF não existir em disco.
    :raises TypeError: Se o tipo do parâmetro pdf_origem for inválido.
    """
    if isinstance(pdf_origem, bytes):
        doc_origem = fitz.open(stream=pdf_origem, filetype="pdf")
    elif isinstance(pdf_origem, io.BytesIO):
        doc_origem = fitz.open(stream=pdf_origem.getvalue(), filetype="pdf")
    elif isinstance(pdf_origem, (Path, str)):
        caminho = Path(pdf_origem)
        if not caminho.exists():
            raise FileNotFoundError(f"Arquivo não encontrado: {caminho}")
        doc_origem = fitz.open(str(caminho))
    else:
        raise TypeError(f"Tipo de entrada não suportado: {type(pdf_origem)}")

    doc_truncado = fitz.open()

    total_pags = min(len(doc_origem), max_paginas)
    for i in range(total_pags):
        doc_truncado.insert_pdf(doc_origem, from_page=i, to_page=i)

    doc_origem.close()

    pdf_bytes = doc_truncado.tobytes(garbage=4, deflate=True)
    doc_truncado.close()

    return pdf_bytes
